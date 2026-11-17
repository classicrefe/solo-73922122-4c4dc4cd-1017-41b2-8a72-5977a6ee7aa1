"""自动采集写入控制器：停止 / 切月的状态机。

修复的缺陷（原实现）：停止请求对采集线程 join(超时) 后便宣称"已停止"，
并直接清停止标志、起新线程。若此时甲月某次独立 SQL 写入正被数据库锁挡住，
旧线程其实还活着；停止标志被新线程清掉后，旧线程锁一解除就永远不再退出
——甲乙两月同时写入，而页面只看得到新线程，报"自动采集运行中"。

本控制器的不变量：

  * 同一时刻至多一个采集线程；
  * 控制请求只改状态，**绝不 join / 等待在途写入**，立即返回真实状态；
  * 只有在途写入结束、旧线程完全退出（其退出回调里）之后，
    才会宣告"已停止"或启动待切换的新月份；
  * 停止等待中再次取消（enabled=false）会撤销待启动月份。

状态机（state() 的 phase 字段）：

  idle      无采集线程；
  running   采集线程运行中，正在写 month；
  stopping  已请求停止，旧线程尚未退出（可能有在途写入被锁挡住）；
            若同时请求了切月，pending_month 为待启动月份，
            旧线程退出后自动启动。
"""

from __future__ import annotations

import random
import threading
from typing import Callable

from db import log

# 写入操作的随机权重：新增为主，兼有更正与删除
OPS = ["insert"] * 5 + ["update"] * 3 + ["delete"] * 2


class AutoWriter:
    """自动采集写入的启停 / 切月控制（进程内单例）。"""

    def __init__(self, write_fn: Callable[[str, str], dict]):
        self._write_fn = write_fn
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._month: str | None = None            # 当前采集线程的目标月份
        self._interval_ms = 300
        self._pending_month: str | None = None    # 待启动月份（旧线程退出后启动）
        self._pending_interval_ms = 300
        self._writes = 0                          # 本轮采集已完成的写入数
        self._in_flight = False                   # 是否有在途写入（可能被锁挡住）

    # ------------------------------------------------------------ 线程侧
    def _loop(self, month: str):
        try:
            while not self._stop.is_set():
                self._in_flight = True
                try:
                    result = self._write_fn(random.choice(OPS), month)
                    self._writes += 1
                    log(f"[自动采集 {month}] {result}")
                except Exception as exc:  # noqa: BLE001 - 演示链路，失败仅记录
                    log(f"[自动采集 {month}] 写入失败: {exc!r}")
                finally:
                    self._in_flight = False
                self._stop.wait(max(50, self._interval_ms) / 1000)
        finally:
            self._on_thread_exit()

    def _on_thread_exit(self):
        """旧线程完全退出后的收尾：启动待切换月份，或宣告停止。

        这是全系统唯一会把状态推进到 idle / 启动新线程的地方，
        因此"已停止"与"新月启动"一定发生在在途写入结束之后。
        """
        with self._lock:
            self._thread = None
            exited = self._month
            self._month = None
            pending, interval = self._pending_month, self._pending_interval_ms
            self._pending_month = None
            if pending is None:
                log(f"[自动采集] 已停止（{exited} 在途写入已全部结束）")
                return
            try:
                self._stop.clear()
                self._start_locked(pending, interval)
            except Exception as exc:  # noqa: BLE001
                log(f"[自动采集] 待启动月份 {pending} 启动失败: {exc!r}")
                return
            log(f"[自动采集] {exited} 已退出，待启动月份 {pending} 现在开始采集")

    def _start_locked(self, month: str, interval_ms: int):
        """调用方须持有 _lock 且当前无存活线程。"""
        t = threading.Thread(
            target=self._loop, args=(month,),
            daemon=True, name=f"auto-writer-{month}",
        )
        t.start()
        self._thread = t
        self._month = month
        self._interval_ms = interval_ms
        self._writes = 0

    # ------------------------------------------------------------ 控制侧
    def control(self, enabled: bool, month: str | None, interval_ms: int) -> dict:
        """处理启动 / 停止 / 切月请求。只改状态，立即返回，绝不等待在途写入。"""
        with self._lock:
            alive = self._thread is not None
            if enabled:
                if not alive:
                    # 空闲 → 直接启动
                    self._stop.clear()
                    self._start_locked(month, interval_ms)
                    log(f"[自动采集] 已启动，目标月份 {month}，间隔 {interval_ms}ms")
                elif not self._stop.is_set() and self._month == month:
                    # 同月重复启动：不重启线程，仅调整间隔
                    self._interval_ms = interval_ms
                    log(f"[自动采集] {month} 运行中，间隔调整为 {interval_ms}ms")
                else:
                    # 切月（或停止中改主意）：先停旧月，登记待启动月份；
                    # 不等待在途写入，旧线程退出回调里再真正启动新月
                    self._stop.set()
                    self._pending_month = month
                    self._pending_interval_ms = interval_ms
                    log(
                        f"[自动采集] 切月请求：{self._month} 停止中，{month} "
                        f"登记为待启动（不等待在途写入，旧线程退出后自动切换）"
                    )
            else:
                if alive and not self._stop.is_set():
                    self._stop.set()
                    log(
                        f"[自动采集] 停止请求已受理：{self._month} 停止中"
                        f"（不等待在途写入，线程退出后才算停止）"
                    )
                # 停止等待中再次取消 → 撤销待启动月份
                if self._pending_month is not None:
                    log(f"[自动采集] 取消待启动月份 {self._pending_month}")
                    self._pending_month = None
            return self._state_locked()

    # ------------------------------------------------------------ 状态
    def state(self) -> dict:
        with self._lock:
            return self._state_locked()

    def _state_locked(self) -> dict:
        if self._thread is None:
            phase = "idle"
        elif self._stop.is_set():
            phase = "stopping"
        else:
            phase = "running"
        return {
            "phase": phase,                    # idle / running / stopping
            "running": phase == "running",
            "month": self._month,              # 当前（仍在写的）月份
            "pending_month": self._pending_month,  # 待启动月份
            "interval_ms": self._interval_ms,
            "in_flight": self._in_flight,      # 有在途写入（可能正被锁挡住）
            "writes": self._writes,            # 本轮采集已完成写入数
        }
