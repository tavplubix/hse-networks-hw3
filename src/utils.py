import inspect
import os
from datetime import datetime
from functools import wraps


def debug(args, end="\n", depth=1):
    filename = os.path.basename(inspect.stack()[depth][1])
    line = inspect.stack()[depth][2]
    function_name = inspect.stack()[depth][3]
    time = datetime.now()
    current_time = time.strftime("%H:%M:%S")
    current_date = time.strftime("%d-%m-%Y")
    print(f"{current_date} {current_time} [{function_name} @ {filename}:{line}] {args}", end=end)


def log(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        debug(f"Entered function {function.__name__} with args = {args}, kwargs = {kwargs}")
        result = function(*args, **kwargs)
        debug(f"Exited function")
        return result

    return wrapper