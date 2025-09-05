from __future__ import annotations

import logging
import time
from collections.abc import Iterable, Iterator
from typing import Callable, Optional, TypeVar

T = TypeVar("T")


class ProgressLogger:
    def __init__(
        self,
        total: Optional[int],
        label: str,
        step_every: int = 10_000,
        secs_every: float = 5.0,
        enable_tqdm: bool = False,
        make_desc: Optional[Callable[[int, Optional[int], float], str]] = None,
    ) -> None:
        self.total = total
        self.label = label
        self.step_every = step_every
        self.secs_every = secs_every
        self.enable_tqdm = enable_tqdm
        self.make_desc = make_desc
        self._last_log_count = 0
        self._last_log_time = time.time()
        self._start = self._last_log_time
        self._logger = logging.getLogger(__name__)

        # Optional tqdm
        self._tqdm = None
        if enable_tqdm:
            try:
                from tqdm import tqdm  # type: ignore[import-untyped]

                self._tqdm = tqdm(total=total, desc=label, unit="it")
            except Exception:
                self._tqdm = None  # silently fall back to logging

    def _should_log(self, i: int) -> bool:
        if i - self._last_log_count >= self.step_every:
            return True
        now = time.time()
        if now - self._last_log_time >= self.secs_every:
            return True
        return False

    def _fmt(self, i: int) -> str:
        elapsed = time.time() - self._start
        rate = i / elapsed if elapsed > 0 else 0.0
        eta = ""
        if self.total is not None and rate > 0:
            remaining = max(self.total - i, 0)
            eta_secs = remaining / rate
            eta = f" | eta={eta_secs:,.0f}s"
        base = f"{self.label}: {i:,}/{self.total if self.total is not None else '?'} it | {rate:,.0f} it/s | elapsed={elapsed:,.0f}s{eta}"
        if self.make_desc:
            try:
                extra = self.make_desc(i, self.total, elapsed)
                if extra:
                    base += f" | {extra}"
            except Exception:
                pass
        return base

    def wrap(self, it: Iterable[T]) -> Iterator[T]:
        for i, item in enumerate(it, 1):
            if self._tqdm is not None:
                self._tqdm.update(1)
            if self._should_log(i) and self._tqdm is None:
                self._logger.info(self._fmt(i))
                self._last_log_count = i
                self._last_log_time = time.time()
            yield item
        # final log
        if self._tqdm is None:
            self._logger.info(self._fmt(i if "i" in locals() else 0))
        else:
            try:
                self._tqdm.close()
            except Exception:
                pass
