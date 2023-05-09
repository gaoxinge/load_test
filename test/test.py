import os
import time
import logging
import unittest
from concurrent.futures import ThreadPoolExecutor

from load_test import gen_latency, gen_through_output

os.environ["LOAD_TEST_TIMES"] = "4"

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def fn(arg):
    time.sleep(1)
    return arg


def fn_with_err(arg):
    raise Exception("error")


class ThreadTask:

    def __init__(self, parallel):
        self.executor = ThreadPoolExecutor(max_workers=parallel)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.executor.__exit__(exc_type, exc_val, exc_tb)

    def apply(self, arg):
        future = self.executor.submit(fn, arg)
        return future.result()


class Test(unittest.TestCase):

    def test_gen_latency(self):
        n, t = gen_latency(fn, 1, 1)
        self.assertEqual(n, 1)
        self.assertAlmostEqual(t, 1, delta=0.1)

        n, t = gen_latency(fn, 1, 2)
        self.assertEqual(n, 0)
        self.assertAlmostEqual(t, 1, delta=0.1)

        n, t = gen_latency(fn_with_err, 1, 1)
        self.assertEqual(n, 0)
        self.assertAlmostEqual(t, 0, delta=0.1)

    def test_gen_through_output(self):
        with ThreadTask(parallel=15) as tt:
            fn_with_thread = tt.apply

            n, t = gen_latency(fn_with_thread, 1, 1)
            self.assertEqual(n, 1)
            self.assertAlmostEqual(t, 1, delta=0.1)

            n, p = gen_through_output(fn_with_thread, 1, 1, t)
            self.assertEqual(n, 1)
            self.assertAlmostEqual(p, 15, delta=1)
