# Kaito Minami
# DS4300: Large-scale Information Storage and Retrieval

import time
from functools import wraps

def perf_tester(func):
    """ wrapper to performance test how many times a function runs in one second """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        count = 0

        while time.time() - start_time < 1:
            func(*args, **kwargs)
            count += 1

        execution_time = time.time() - start_time
        if func.__name__ == 'postTweets' or func.__name__ == 'postTweetsStr1':
            print(f"Function {func.__name__} ran {1000000.0 / float(execution_time / count)} times in 1 second.")
        else:
            print(f"Function {func.__name__} ran {count} times in 1 second.")
        print(f"Average execution time: {execution_time / count:.5f} seconds per function call")
        return count

    return wrapper
