import time
from siesta_framework.core.config import get_system_config

def timed(func, prefix:str="",  *args, **kwargs):
    do_time = get_system_config().get("enable_timing", False)
    if not do_time:
        return func(*args, **kwargs)
    else:    
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        print(f"{prefix}{func.__name__} took {end - start:.6f} seconds")
        return result