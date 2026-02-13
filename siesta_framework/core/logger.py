import time
from typing import Any, TypeVar, Callable, Any, Dict, ParamSpec
from siesta_framework.core.config import get_system_config
import logging



P = ParamSpec("P") # P captures the parameter types (args and kwargs)
R = TypeVar("R") # R captures the return type

logger = logging.getLogger("Timer")


def timed(func: Callable[P, R], prefix:str="",  *args: P.args, **kwargs: P.kwargs) -> R:
    do_time = get_system_config().get("enable_timing", False)
    if not do_time:
        return func(*args, **kwargs)
    else:    
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        logger.info(f"{prefix}{func.__name__} took {end - start:.6f} seconds")
        return result