from __future__ import annotations

from dataclasses import dataclass, replace
import logging
from typing import Any, Callable, Optional


@dataclass
class PrefixedLogger:
    logger: logging.Logger
    prefix: Optional[str] = None

    def setLevel(self, level: int) -> None:
        self.logger.setLevel(level)

    def log(
        self, level: int, msg: str, *args: Any, stacklevel: int = 2, **kwargs: Any
    ) -> None:
        if self.prefix:
            msg = f"%s: {msg}"
            args = (self.prefix, *args)
        self.logger.log(level, msg, *args, stacklevel=stacklevel, **kwargs)

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self.log(logging.DEBUG, msg, *args, stacklevel=3, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self.log(logging.INFO, msg, *args, stacklevel=3, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self.log(logging.WARNING, msg, *args, stacklevel=3, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self.log(logging.ERROR, msg, *args, stacklevel=3, **kwargs)

    def critical(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self.log(logging.CRITICAL, msg, *args, stacklevel=3, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        kwargs["exc_info"] = True
        self.log(logging.ERROR, msg, *args, stacklevel=3, **kwargs)

    def sublogger(self, prefix: str) -> PrefixedLogger:
        if self.prefix is None:
            p2 = prefix
        else:
            p2 = f"{self.prefix}: {prefix}"
        return replace(self, prefix=p2)


log = PrefixedLogger(logging.getLogger("backups2datalad"))


def quiet_filter(deflevel: int) -> Callable[[logging.LogRecord], bool]:
    def filterfunc(record: logging.LogRecord) -> bool:
        if record.name == "backups2datalad":
            return record.levelno >= logging.DEBUG
        else:
            return record.levelno >= deflevel

    return filterfunc
