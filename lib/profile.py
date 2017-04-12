import time
import math

class Profile(object):
    @staticmethod
    def run_3(func, ip, args, **params):
        startTime = time.time()
        result = func(ip, args, **params)
        return time.time() - startTime, result

    @staticmethod
    def run_4(func, ip, doc, args, **params):
        startTime = time.time()
        result = func(ip, doc, args, **params)
        return time.time() - startTime, result

    @staticmethod
    def get_bin(x):
        if x < 32: return 0
        if x > 16384: return 10
        return int(math.floor(math.log(x)/math.log(2))) - 4


