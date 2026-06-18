"""
Утилиты: экспоненциальный backoff, Circuit Breaker, трекинг здоровья.
"""

import time
import random
import logging
import os
from datetime import datetime
from functools import wraps


# ---------------------------------------------------------------------------
# Логгер
# ---------------------------------------------------------------------------

def setup_logger(name: str, log_file: str, level=logging.DEBUG) -> logging.Logger:
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        # Консоль
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(fmt)
        logger.addHandler(ch)

        # Файл
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger


# ---------------------------------------------------------------------------
# Экспоненциальный backoff
# ---------------------------------------------------------------------------

def calc_backoff(attempt: int, base: float = 5.0, maximum: float = 300.0, jitter: bool = True) -> float:
    """
    Вычисляет задержку с экспоненциальным ростом и случайным джиттером.
    attempt=0 → ~5 с, attempt=1 → ~10 с, attempt=2 → ~20 с … max 300 с.
    """
    delay = min(base * (2 ** attempt), maximum)
    if jitter:
        delay *= 0.6 + random.random() * 0.4   # 60–100 %
    return delay


def sleep_backoff(attempt: int, base: float = 5.0, maximum: float = 300.0,
                  label: str = "", logger: logging.Logger = None) -> float:
    delay = calc_backoff(attempt, base, maximum)
    msg = f"⏳ Пауза {delay:.1f} с перед попыткой #{attempt + 2}"
    if label:
        msg += f" ({label})"
    if logger:
        logger.warning(msg)
    else:
        print(msg)
    time.sleep(delay)
    return delay


# ---------------------------------------------------------------------------
# Декоратор retry
# ---------------------------------------------------------------------------

def with_retry(
    max_attempts: int = 3,
    base_delay: float = 5.0,
    max_delay: float = 120.0,
    retryable=(Exception,),
    label: str = "",
    logger: logging.Logger = None,
):
    """
    Декоратор: повторяет вызов при исключении из `retryable`.
    Возвращает None если исчерпаны все попытки (не пробрасывает).
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except retryable as exc:
                    is_last = attempt == max_attempts - 1
                    tag = label or func.__name__
                    msg = f"⚠️ [{tag}] попытка {attempt + 1}/{max_attempts}: {type(exc).__name__}: {exc}"
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    if is_last:
                        return None
                    sleep_backoff(attempt, base_delay, max_delay, tag, logger)
            return None
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Circuit Breaker
# ---------------------------------------------------------------------------

class CircuitBreaker:
    """
    Автомат состояний для защиты от каскадных сбоев.

    CLOSED   → нормальная работа
    OPEN     → сервис недоступен, вызовы отклоняются немедленно
    HALF_OPEN → пробный вызов после таймаута восстановления
    """

    CLOSED    = "closed"
    OPEN      = "open"
    HALF_OPEN = "half-open"

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 120.0,
        name: str = "CB",
        logger: logging.Logger = None,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self.logger = logger

        self._failures = 0
        self._opened_at: float | None = None
        self.state = self.CLOSED

    # -- публичный интерфейс --

    def is_open(self) -> bool:
        """True → вызов нужно пропустить."""
        if self.state == self.OPEN:
            if time.monotonic() - self._opened_at >= self.recovery_timeout:
                self._transition(self.HALF_OPEN)
                return False
            return True
        return False

    def record_success(self):
        if self.state != self.CLOSED:
            self._transition(self.CLOSED)
        self._failures = 0

    def record_failure(self):
        self._failures += 1
        if self._failures >= self.failure_threshold and self.state != self.OPEN:
            self._transition(self.OPEN)

    def reset(self):
        self._failures = 0
        self._opened_at = None
        self.state = self.CLOSED

    # -- внутреннее --

    def _transition(self, new_state: str):
        old = self.state
        self.state = new_state
        if new_state == self.OPEN:
            self._opened_at = time.monotonic()
        msg = f"[{self.name}] {old} → {new_state} (failures={self._failures})"
        if self.logger:
            self.logger.warning(msg)
        else:
            print(msg)

    def __str__(self):
        return f"CircuitBreaker({self.name}, state={self.state}, failures={self._failures})"


# ---------------------------------------------------------------------------
# Трекер здоровья
# ---------------------------------------------------------------------------

class HealthTracker:
    """Собирает метрики работы монитора."""

    def __init__(self):
        self.start_time = datetime.now()
        self.total_checks = 0
        self.total_errors = 0
        self.consecutive_failures = 0
        self.reauth_count = 0
        self.last_success: datetime | None = None

    def ok(self):
        self.total_checks += 1
        self.consecutive_failures = 0
        self.last_success = datetime.now()

    def fail(self):
        self.total_checks += 1
        self.total_errors += 1
        self.consecutive_failures += 1

    def reauth(self):
        self.reauth_count += 1

    @property
    def success_rate(self) -> float:
        if self.total_checks == 0:
            return 100.0
        return (self.total_checks - self.total_errors) / self.total_checks * 100

    def summary(self) -> str:
        uptime = datetime.now() - self.start_time
        h, rem = divmod(int(uptime.total_seconds()), 3600)
        m, s = divmod(rem, 60)
        last_ok = (
            self.last_success.strftime("%H:%M:%S") if self.last_success else "—"
        )
        return (
            f"uptime={h}h{m}m{s}s | "
            f"checks={self.total_checks} | "
            f"errors={self.total_errors} | "
            f"ok={self.success_rate:.1f}% | "
            f"reauths={self.reauth_count} | "
            f"last_ok={last_ok}"
        )