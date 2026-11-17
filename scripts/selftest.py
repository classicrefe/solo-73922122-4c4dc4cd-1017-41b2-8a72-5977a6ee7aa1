#!/usr/bin/env python3
"""
端到端自测（真实 PostgreSQL + 真实 HTTP API + 真实进程崩溃）：

  1. 慢速复制 + 并发直接 SQL 增删改 → 切换后父表内容与期望逐行一致
  2. 一次只迁一个月（并发启动第二个 → 409）
  3. 已完成月份再次迁移 → 409 且数据不变
  4. copying / finalizing 阶段故障注入 → 父表完整可查 → 重试完成
  5. SIGKILL 应用进程（复制进行中）→ 父表仍完整可查可写 → 重启后重试完成
  6. 自动采集停止/切月：在途写入被 SHARE 锁挡住时，控制请求立即返回
     "停止中/待启动"真实状态；锁解除后旧线程退出、待启动月自动接管；
     停止等待中再次取消可撤销待启动月份
  7. 自动采集连续控制：快速启停/切月/取消，响应快速且状态自洽，收敛到最后意图
  8. 收尾检查：默认分区无残留、无重复主键、触发器/增量清理干净

运行：DATABASE_URL=... python3 scripts/selftest.py
（脚本自己拉起/杀掉 uvicorn 进程；要求 DATABASE_URL 指向已初始化的库）
"""

from __future__ import annotations

import os
import random
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import psycopg
import requests

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8010")
DSN = os.environ.get(
    "DATABASE_URL", "postgresql://envmon@127.0.0.1:5544/envmon"
)
APP_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app"))
APP_PORT = int(os.environ.get("APP_PORT", "8010"))
STATIONS = [f"ST{i:03d}" for i in range(1, 9)]

_failures: list[str] = []


def check(name: str, cond: bool, detail: str = ""):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail else ""), flush=True)
    if not cond:
        _failures.append(f"{name}: {detail}")


def db():
    return psycopg.connect(DSN, autocommit=True, options="-c timezone=UTC")


def month_range(month: str):
    lo = datetime.fromisoformat(month + "-01").replace(tzinfo=timezone.utc)
    hi = (lo.replace(day=28) + timedelta(days=8)).replace(day=1)
    return lo, hi


def snapshot(month: str) -> dict:
    """父表该月全部内容（ground truth，直接 SQL）。"""
    lo, hi = month_range(month)
    with db() as c:
        rows = c.execute(
            "SELECT station_id, ts, value FROM observations"
            " WHERE ts >= %s AND ts < %s",
            (lo, hi),
        ).fetchall()
    return {(r[0], r[1]): r[2] for r in rows}


def default_count(month: str) -> int:
    lo, hi = month_range(month)
    with db() as c:
        return c.execute(
            "SELECT count(*) FROM observations_default WHERE ts >= %s AND ts < %s",
            (lo, hi),
        ).fetchone()[0]


def run_row(month: str):
    with db() as c:
        return c.execute(
            "SELECT status, copied_rows, applied_delta, error"
            " FROM migration_runs WHERE month = %s::date", (month + "-01",)
        ).fetchone()


def partition_name(month: str) -> str:
    return "observations_" + month.replace("-", "_")


def partition_exists(month: str) -> bool:
    with db() as c:
        return bool(
            c.execute(
                "SELECT 1 FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid"
                " WHERE i.inhparent = 'observations'::regclass AND c.relname = %s",
                (partition_name(month),),
            ).fetchone()
        )


def partition_count(month: str) -> int:
    with db() as c:
        return c.execute(
            f"SELECT count(*) FROM {partition_name(month)}"
        ).fetchone()[0]


class ChaosWriter(threading.Thread):
    """独立采集连接：直接 SQL 随机 新增/更正/删除，并维护期望结果。"""

    def __init__(self, month: str, expected: dict):
        super().__init__(daemon=True)
        self.month = month
        self.expected = expected
        self.stop_event = threading.Event()
        self.ops = 0
        self.errors: list[str] = []
        self.lo, self.hi = month_range(month)

    def run(self):
        try:
            conn = psycopg.connect(DSN, autocommit=True,
                                   options="-c timezone=UTC")
        except Exception as e:  # noqa: BLE001
            self.errors.append(f"connect: {e!r}")
            return
        with conn:
            while not self.stop_event.is_set():
                op = random.choices(["insert", "update", "delete"],
                                    weights=[5, 3, 2])[0]
                try:
                    self._do(conn, op)
                    self.ops += 1
                except Exception as e:  # noqa: BLE001
                    self.errors.append(f"{op}: {e!r}")
                time.sleep(random.uniform(0.01, 0.06))

    def _do(self, conn, op: str):
        if op == "insert":
            station = random.choice(STATIONS)
            span = int((self.hi - self.lo).total_seconds())
            ts = self.lo + timedelta(seconds=random.randrange(span))
            value = random.randint(0, 100)
            row = conn.execute(
                "INSERT INTO observations (station_id, ts, value)"
                " VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
                " RETURNING station_id, ts, value",
                (station, ts, value),
            ).fetchone()
            if row:
                self.expected[(row[0], row[1])] = row[2]
        elif op == "update":
            row = conn.execute(
                "SELECT station_id, ts FROM observations"
                " WHERE ts >= %s AND ts < %s ORDER BY random() LIMIT 1",
                (self.lo, self.hi),
            ).fetchone()
            if row:
                value = random.randint(0, 100)
                conn.execute(
                    "UPDATE observations SET value = %s"
                    " WHERE station_id = %s AND ts = %s",
                    (value, row[0], row[1]),
                )
                self.expected[(row[0], row[1])] = value
        else:
            row = conn.execute(
                "SELECT station_id, ts FROM observations"
                " WHERE ts >= %s AND ts < %s ORDER BY random() LIMIT 1",
                (self.lo, self.hi),
            ).fetchone()
            if row:
                conn.execute(
                    "DELETE FROM observations WHERE station_id = %s AND ts = %s",
                    (row[0], row[1]),
                )
                self.expected.pop((row[0], row[1]), None)


# ------------------------------------------------------------------ app 进程
def post_auto(body: dict) -> requests.Response:
    return requests.post(f"{BASE}/api/demo/auto", json=body, timeout=10)


def auto_state() -> dict:
    r = requests.get(f"{BASE}/api/state", timeout=5)
    r.raise_for_status()
    return r.json()["auto_writer"]


def month_row_count(month: str) -> int:
    lo, hi = month_range(month)
    with db() as c:
        return c.execute(
            "SELECT count(*) FROM observations WHERE ts >= %s AND ts < %s",
            (lo, hi),
        ).fetchone()[0]


def wait_auto(want_phase: str, want_month: str | None = None, timeout=20) -> dict:
    """等待自动采集进入指定阶段（可附带月份），返回最终状态。"""
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        last = auto_state()
        if last["phase"] == want_phase and (
            want_month is None or last["month"] == want_month
        ):
            return last
        time.sleep(0.2)
    raise RuntimeError(f"自动采集等待 {want_phase}/{want_month} 超时，当前 {last}")


def check_auto_coherent(st: dict) -> bool:
    """自动采集状态的基本一致性（任何时刻都应满足）。"""
    if st["phase"] not in ("idle", "running", "stopping"):
        return False
    if st["phase"] == "idle":
        return st["month"] is None and st["pending_month"] is None
    if st["phase"] == "running":
        return st["month"] is not None and st["pending_month"] is None
    return st["month"] is not None  # stopping：旧线程尚未退出


_app_proc: subprocess.Popen | None = None


def start_app() -> subprocess.Popen:
    global _app_proc
    logf = open("/tmp/pg-runtime/app.log", "ab")
    _app_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app",
         "--host", "127.0.0.1", "--port", str(APP_PORT)],
        cwd=APP_DIR, stdout=logf, stderr=subprocess.STDOUT,
        env={**os.environ, "DATABASE_URL": DSN},
    )
    wait_healthy()
    return _app_proc


def crash_app():
    """通过调试端点让应用硬退出（os._exit，等价于宕机/SIGKILL 的效果）。"""
    global _app_proc
    try:
        requests.post(f"{BASE}/api/debug/crash", timeout=3)
    except requests.RequestException:
        pass
    if _app_proc:
        try:
            _app_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
    _app_proc = None
    # 等端口彻底拒绝连接
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            requests.get(f"{BASE}/api/state", timeout=1)
            time.sleep(0.3)
        except requests.RequestException:
            return
    raise RuntimeError("应用进程未能退出")


def wait_healthy(timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(f"{BASE}/api/state", timeout=2).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(0.3)
    raise RuntimeError("应用未在预期时间内就绪")


def api_migrate(month: str, **kw) -> requests.Response:
    body = {"month": month, "batch_size": 200, "sleep_ms": 80}
    body.update(kw)
    return requests.post(f"{BASE}/api/migrate", json=body, timeout=10)


def wait_status(month: str, want: set[str], timeout=180) -> str:
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = run_row(month)
        if row and row[0] in want:
            return row[0]
        time.sleep(0.4)
    raise RuntimeError(f"{month} 等待状态 {want} 超时，当前 {run_row(month)}")


def verify_month(month: str, expected: dict, label: str):
    """迁移完成后的一致性校验（全部用直接 SQL 独立验证）。"""
    got = snapshot(month)
    check(f"{label}: 父表内容与期望逐行一致", got == expected,
          f"父表 {len(got)} 行 vs 期望 {len(expected)} 行"
          + ("" if got == expected else
             f"，差异样例 {list((got.items() ^ expected.items()))[:3]}"))
    check(f"{label}: 默认分区无残留", default_count(month) == 0,
          f"剩余 {default_count(month)}")
    check(f"{label}: 月分区已挂载", partition_exists(month))
    if partition_exists(month):
        check(f"{label}: 分区行数 == 父表行数",
              partition_count(month) == len(got),
              f"{partition_count(month)} vs {len(got)}")
    with db() as c:
        dup = c.execute(
            "SELECT count(*) - count(DISTINCT (station_id, ts))"
            " FROM observations WHERE ts >= %s AND ts < %s",
            month_range(month),
        ).fetchone()[0]
        trg = c.execute(
            "SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'obs_delta%'"
        ).fetchone()[0]
        delta = c.execute(
            "SELECT count(*) FROM migration_delta WHERE month = %s::date",
            (month + "-01",),
        ).fetchone()[0]
    check(f"{label}: 父表主键无重复", dup == 0, f"重复 {dup}")
    check(f"{label}: 增量触发器已清理", trg == 0, f"剩余 {trg}")
    check(f"{label}: 增量日志已清理", delta == 0, f"剩余 {delta}")
    r = requests.get(f"{BASE}/api/verify", params={"month": month}, timeout=10)
    check(f"{label}: /api/verify 判定一致", r.ok and r.json()["consistent"],
          r.text[:200])


# ---------------------------------------------------------------------- 用例
def test_auto_writer_blocked_stop_switch(month_a: str, month_b: str):
    """核心场景：甲月写入被数据库锁挡住时，停止/切月必须如实报告且不等待。"""
    print(f"\n== 用例：自动采集停止/切月 —— 在途写入被锁阻塞（{month_a}→{month_b}）==",
          flush=True)
    post_auto({"enabled": False})
    try:
        wait_auto("idle", timeout=10)
    except RuntimeError:
        pass
    # 1) 启动甲月自动采集，确认真的在写
    r = post_auto({"enabled": True, "month": month_a, "interval_ms": 100})
    st = r.json()
    check("启动甲月自动采集", r.ok and st["phase"] == "running"
          and st["month"] == month_a, r.text[:200])
    st = wait_auto("running", month_a, timeout=10)
    deadline = time.time() + 10
    while st["writes"] == 0 and time.time() < deadline:
        time.sleep(0.2)
        st = auto_state()
    check("甲月已有写入完成", st["writes"] > 0, f"writes={st['writes']}")

    # 2) 另一事务对父表加 SHARE 锁（与迁移 finalizing 同款），采集写入被挡住
    lock_conn = psycopg.connect(DSN, options="-c timezone=UTC")
    try:
        lock_conn.execute("LOCK TABLE observations IN SHARE MODE")
        # 3) 等采集连接的在途写入真正被锁挡住（未授予的锁 + 应用侧 in_flight）
        blocked = False
        deadline = time.time() + 15
        while time.time() < deadline:
            with db() as c:
                n = c.execute(
                    "SELECT count(*) FROM pg_locks"
                    " WHERE relation = 'observations'::regclass AND NOT granted"
                ).fetchone()[0]
            if n > 0 and auto_state()["in_flight"]:
                blocked = True
                break
            time.sleep(0.1)
        check("在途写入被数据库锁挡住（未授予锁 + in_flight）", blocked)

        # 4) 停止请求：不得等待被阻塞的写入，不得宣称已停止
        t0 = time.monotonic()
        r = post_auto({"enabled": False})
        dt = time.monotonic() - t0
        st = r.json()
        check("停止请求不等待被阻塞的写入（快速返回）", dt < 2.0, f"{dt:.2f}s")
        check("返回真实状态：停止中而非已停止",
              r.ok and st["phase"] == "stopping" and st["month"] == month_a
              and not st["running"] and st["pending_month"] is None,
              r.text[:200])
        st = auto_state()
        check("状态接口同样显示停止中（未宣称已停止）",
              st["phase"] == "stopping" and st["month"] == month_a, str(st))

        # 5) 锁仍持有：请求切到乙月 → 只能登记待启动，不得启动乙月
        t0 = time.monotonic()
        r = post_auto({"enabled": True, "month": month_b, "interval_ms": 100})
        dt = time.monotonic() - t0
        st = r.json()
        check("切月请求同样快速返回", dt < 2.0, f"{dt:.2f}s")
        check("乙月登记为待启动，甲月仍是当前月",
              r.ok and st["phase"] == "stopping" and st["month"] == month_a
              and st["pending_month"] == month_b, r.text[:200])
        cnt_b = month_row_count(month_b)
        time.sleep(1.2)
        check("锁未解除前乙月无写入（新月未启动）",
              month_row_count(month_b) == cnt_b,
              f"{cnt_b} → {month_row_count(month_b)}")
        check("停止等待期间页面仍显示甲月为当前月",
              auto_state()["month"] == month_a)

        # 6) 停止等待中再次取消 → 撤销待启动月份
        r = post_auto({"enabled": False})
        st = r.json()
        check("停止等待中再次取消：撤销待启动月份",
              r.ok and st["phase"] == "stopping" and st["month"] == month_a
              and st["pending_month"] is None, r.text[:200])
        # 7) 重新登记切月，确认可再次排队
        r = post_auto({"enabled": True, "month": month_b, "interval_ms": 100})
        st = r.json()
        check("可重新登记待启动乙月",
              r.ok and st["pending_month"] == month_b, r.text[:200])
    finally:
        lock_conn.rollback()  # 8) 解锁：在途写入得以完成
        lock_conn.close()

    # 9) 解锁后：旧线程退出 → 待启动的乙月自动启动
    try:
        st = wait_auto("running", month_b, timeout=20)
        check("解锁后旧线程退出、待启动乙月自动启动", True,
              f"phase={st['phase']} month={st['month']}")
    except RuntimeError as e:
        check("解锁后旧线程退出、待启动乙月自动启动", False, str(e))
        return
    # 10) 乙月持续写入、甲月不再写（在途那笔已随解锁完成，之后不再有）
    cnt_a, cnt_b = month_row_count(month_a), month_row_count(month_b)
    time.sleep(1.5)
    check("解锁后乙月持续写入", month_row_count(month_b) > cnt_b,
          f"{cnt_b} → {month_row_count(month_b)}")
    check("甲月已停止写入", month_row_count(month_a) == cnt_a,
          f"{cnt_a} → {month_row_count(month_a)}")
    # 11) 正常停止（无锁）：旧线程退出后进入 idle
    post_auto({"enabled": False})
    try:
        wait_auto("idle", timeout=10)
        check("无锁时停止后进入已停止(idle)", True)
    except RuntimeError as e:
        check("无锁时停止后进入已停止(idle)", False, str(e))


def test_auto_writer_rapid_control(month_a: str, month_b: str):
    """连续控制：快速启停/切月/取消，响应须立即且状态自洽，最终收敛到最后意图。"""
    print(f"\n== 用例：自动采集连续控制（{month_a}/{month_b}）==", flush=True)
    post_auto({"enabled": False})
    try:
        wait_auto("idle", timeout=10)
    except RuntimeError:
        pass
    seq = [
        {"enabled": True, "month": month_a, "interval_ms": 100},
        {"enabled": True, "month": month_b, "interval_ms": 100},  # 切月
        {"enabled": False},                                       # 取消/撤销待启动
        {"enabled": True, "month": month_b, "interval_ms": 100},
        {"enabled": True, "month": month_a, "interval_ms": 100},  # 再切回甲月
        {"enabled": False},
        {"enabled": True, "month": month_a, "interval_ms": 100},
    ]
    all_ok = True
    for i, body in enumerate(seq):
        t0 = time.monotonic()
        r = post_auto(body)
        dt = time.monotonic() - t0
        ok = r.ok and dt < 2.0 and check_auto_coherent(r.json())
        all_ok = all_ok and ok
        if not ok:
            check(f"连续控制第{i+1}步 {body}", False,
                  f"rc={r.status_code} dt={dt:.2f}s body={r.text[:150]}")
    check("连续控制每步都快速返回且状态自洽", all_ok)
    # 最终收敛到最后意图：甲月运行中
    try:
        st = wait_auto("running", month_a, timeout=20)
        check("连续控制后收敛到最后指定的月份运行", True,
              f"month={st['month']}")
    except RuntimeError as e:
        check("连续控制后收敛到最后指定的月份运行", False, str(e))
        return
    # 最后停掉：进入 idle 后任何月份都不再有写入
    post_auto({"enabled": False})
    try:
        wait_auto("idle", timeout=10)
    except RuntimeError as e:
        check("连续控制收尾停止进入 idle", False, str(e))
        return
    check("连续控制收尾停止进入 idle", True)
    cnt_a, cnt_b = month_row_count(month_a), month_row_count(month_b)
    time.sleep(1.0)
    check("停止后甲乙两月都不再写入",
          month_row_count(month_a) == cnt_a and month_row_count(month_b) == cnt_b)


def test_happy_path(month: str):
    print(f"\n== 用例1：慢速复制 + 并发直接SQL增删改（{month}）==", flush=True)
    expected = snapshot(month)
    base_rows = len(expected)
    r = api_migrate(month, batch_size=200, sleep_ms=120)
    check("迁移启动成功", r.ok, r.text[:200])
    # 一次只迁一个月
    r2 = api_migrate(NEXT_MONTHS[1])
    check("迁移进行中再启动另一个月被拒绝(409)", r2.status_code == 409,
          f"rc={r2.status_code}")
    writer = ChaosWriter(month, expected)
    writer.start()
    status = wait_status(month, {"done", "failed"})
    writer.stop_event.set()
    writer.join(timeout=10)
    check("迁移完成", status == "done", f"status={status} err={run_row(month)}")
    check("并发写入无错误", not writer.errors,
          "; ".join(writer.errors[:3]))
    print(f"  并发写入 {writer.ops} 次；迁移前 {base_rows} 行 → 期望 {len(expected)} 行")
    verify_month(month, expected, "用例1")
    # 幂等：已完成月份再次操作不再搬迁
    before = snapshot(month)
    r3 = api_migrate(month)
    check("已完成月份再次迁移被拒绝(409)", r3.status_code == 409, f"rc={r3.status_code}")
    check("重复操作后数据不变", snapshot(month) == before)


def test_fail_and_retry(month: str, fail_at: str):
    print(f"\n== 用例：{fail_at} 阶段故障注入后重试（{month}）==", flush=True)
    expected = snapshot(month)
    r = api_migrate(month, fail_at=fail_at, sleep_ms=40)
    check("迁移启动成功", r.ok, r.text[:200])
    status = wait_status(month, {"failed", "done"}, timeout=120)
    check("故障注入后状态为 failed", status == "failed", f"status={status}")
    # 父表在失败后仍完整可查
    got = snapshot(month)
    check("失败后父表内容完整不变", got == expected,
          f"{len(got)} vs {len(expected)}")
    check("失败后默认分区仍持有全部数据", default_count(month) == len(expected))
    # 重试（无故障注入）+ 并发写入
    r2 = api_migrate(month, sleep_ms=60)
    check("重试启动成功", r2.ok, r2.text[:200])
    # 等重试真正接管：状态先从 failed 翻转为活跃阶段，
    # 否则 wait_status 可能读到上一次故障注入残留的 failed
    deadline = time.time() + 30
    while time.time() < deadline:
        row = run_row(month)
        if row and row[0] != "failed":
            break
        time.sleep(0.2)
    writer = ChaosWriter(month, expected)
    writer.start()
    status = wait_status(month, {"done", "failed"})
    writer.stop_event.set()
    writer.join(timeout=10)
    check("重试后迁移完成", status == "done", f"status={status}")
    check("重试期间并发写入无错误", not writer.errors,
          "; ".join(writer.errors[:3]))
    verify_month(month, expected, f"重试({fail_at})")


def test_process_kill(month: str):
    print(f"\n== 用例：复制进行中应用进程硬退出（{month}）==", flush=True)
    expected = snapshot(month)
    r = api_migrate(month, batch_size=150, sleep_ms=150)
    check("迁移启动成功", r.ok, r.text[:200])
    # 等复制真正开始
    deadline = time.time() + 60
    while time.time() < deadline:
        row = run_row(month)
        if row and row[1] and row[1] > 0:
            break
        time.sleep(0.2)
    row = run_row(month)
    check("复制已开始（copied_rows>0）", bool(row and row[1] and row[1] > 0),
          f"run={row}")
    crash_app()
    time.sleep(1)
    # 应用已死：父表仍完整可查，且采集写入（直接 SQL）仍可提交
    got = snapshot(month)
    check("进程被杀后父表内容完整", got == expected,
          f"{len(got)} vs {len(expected)}")
    probe = ChaosWriter(month, expected)
    probe.start()
    time.sleep(2)
    probe.stop_event.set()
    probe.join(timeout=10)
    check("应用停机期间直接 SQL 写入正常", probe.ops > 0 and not probe.errors,
          f"ops={probe.ops} err={probe.errors[:2]}")
    # 重启应用，重试迁移
    start_app()
    r2 = api_migrate(month, sleep_ms=60)
    check("重启后重试启动成功", r2.ok, r2.text[:200])
    writer = ChaosWriter(month, expected)
    writer.start()
    status = wait_status(month, {"done", "failed"})
    writer.stop_event.set()
    writer.join(timeout=10)
    check("崩溃恢复后迁移完成", status == "done", f"status={status}")
    verify_month(month, expected, "崩溃恢复")


def main():
    global NEXT_MONTHS
    random.seed(20261116)
    with db() as c:
        months = [r[0].isoformat()[:7] for r in c.execute(
            "SELECT (date_trunc('month', ts AT TIME ZONE 'UTC'))::date AS m,"
            "       count(*) FROM observations_default"
            " GROUP BY 1 ORDER BY 1"
        )]
    check("种子数据至少 4 个月份", len(months) >= 4, f"months={months}")
    NEXT_MONTHS = months
    m1, m2, m3, m4 = months[:4]
    # 自动采集用例使用未参与迁移的月份（种子有 6 个月）
    m5, m6 = (months[4], months[5]) if len(months) >= 6 else (m3, m4)

    start_app()
    try:
        r = requests.get(f"{BASE}/api/state", timeout=10)
        check("GET /api/state 可用", r.ok)
        test_happy_path(m1)
        test_fail_and_retry(m2, "copying")
        test_fail_and_retry(m3, "finalizing")
        test_process_kill(m4)
        test_auto_writer_blocked_stop_switch(m5, m6)
        test_auto_writer_rapid_control(m5, m6)
    finally:
        crash_app()

    print("\n" + "=" * 60)
    if _failures:
        print(f"自测失败 {len(_failures)} 项：")
        for f in _failures:
            print("  -", f)
        sys.exit(1)
    print("全部自测通过 ✓")


if __name__ == "__main__":
    main()
