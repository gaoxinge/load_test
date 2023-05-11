import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED

formatter = logging.Formatter(
    '[%(levelname)s] '
    '%(asctime)s | '
    '%(processName)s | '
    'pid:%(process)d | '
    '%(threadName)s | '
    '%(filename)s | '
    'line:%(lineno)d | '
    '%(message)s'
)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)


def _wrapper(fn, res, skip=False):
    def fn0(arg):
        start = time.time()
        try:
            res0 = fn(arg)
            latency = time.time() - start
            if skip or res == res0:
                success = True
                error = None
            else:
                success = False
                error = Exception(f"expect and actual not same: expect {res}, actual {res0}")
        except Exception as e:
            latency = time.time() - start
            success = False
            error = e
        return latency, success, error
    return fn0


def gen_latency(fn, arg, res, skip=False):
    ns = int(os.environ.get("LOAD_TEST_TIMES", "32"))
    fn = _wrapper(fn, res, skip)

    n, total = 0, 0
    for _ in range(ns):
        latency, success, error = fn(arg)
        if success:
            n += 1
        else:
            logger.error("gen latency with error %s", error)
        total += latency
    return n / ns, total / ns


def _gen_through_output(fn, arg, res, parallel, ns, skip=False):
    ns = parallel * ns
    fn = _wrapper(fn, res, skip)

    n, total = 0, 0
    with ThreadPoolExecutor(max_workers=parallel) as executor:
        futures = [executor.submit(fn, arg) for _ in range(ns)]
        done, futures = wait(futures, return_when=ALL_COMPLETED)
        for future in done:
            latency, success, error = future.result()
            if success:
                n += 1
            else:
                logger.error("gen through output with error %s", error)
            total += latency
    return n / ns, total / ns


def gen_through_output(fn, arg, res, latency, skip=False):
    logger.debug("gen through output with latency %s", latency)

    logger.debug("gen through output try to find the parallel with phase one")
    parallel = 1
    while True:
        logger.debug("gen through output by parallel %s and ns 2", parallel)
        _, t = _gen_through_output(fn, arg, res, parallel, 2, skip)
        logger.debug("gen through output by parallel %s and ns 2 with latency %s", parallel, t)
        delta = 2 * latency / parallel
        if t < latency + delta:
            parallel *= 2
        else:
            break

    logger.debug("gen through output try to find the parallel with phase two")
    while parallel > 1:
        logger.debug("gen through output by parallel %s and ns 2", parallel)
        _, t = _gen_through_output(fn, arg, res, parallel, 2, skip)
        logger.debug("gen through output by parallel %s and ns 2 with latency %s", parallel, t)
        delta = latency / parallel
        if t > latency + delta and parallel > 1:
            parallel -= 1
        else:
            break

    ns = int(os.environ.get("LOAD_TEST_TIMES", "32"))
    logger.debug("gen through output by parallel %s and ns %s", parallel, ns)
    n, t = _gen_through_output(fn, arg, res, parallel, ns, skip)
    logger.debug("gen through output by parallel %s and ns %s with latency %s", parallel, ns, t)
    return n, parallel


def module_load_test(fn, name_arg_res_list, skip=False):
    result = []
    for name, arg_fn, res_fn in name_arg_res_list:
        arg = arg_fn()
        res = res_fn()
        n, t = gen_latency(fn, arg, res, skip)
        result.append((name, n, t) if not skip else (name, t))
    return result


def service_load_test(fn, name_arg_res_list, skip=False):
    result = []
    for name, arg_fn, res_fn in name_arg_res_list:
        arg = arg_fn()
        res = res_fn()
        n1, t = gen_latency(fn, arg, res, skip)
        n2, p = gen_through_output(fn, arg, res, t, skip)
        result.append((name, n1, t, n2, p) if not skip else (name, t, p))
    return result
