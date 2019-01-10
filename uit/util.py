"""
********************************************************************************
* Name: util.py
* Author: nswain
* Created On: November 30, 2018
* Copyright: (c) Aquaveo 2018
********************************************************************************
"""
from .exceptions import DpRouteError


def robust(retries=3):
    """
    Robust wrapper for client methods. Will retry, reties times if failed due to DP routing error.
    """  # noqa: E501
    def wrap(func):
        def wrap_f(*args, **kwargs):
            attempts = 1

            last_exception = None

            while attempts <= retries:
                try:
                    return func(*args, **kwargs)
                except RuntimeError as e:
                    # "DP Route error" indicates failure of SSH Tunnel client on UIT Plus server.
                    # Successive calls should work.
                    if 'DP Route error' in str(e):
                        attempts += 1
                        last_exception = e
                        continue
                    else:
                        # Raise other Runtime Errors
                        raise

            kwarg_str = ', '.join(['{}="{}"'.format(k, v) for k, v in kwargs.items()])
            if 'DP Route error' in str(last_exception):
                raise DpRouteError('Max number of retries reached without success for '
                                   'method: {}({}). Last exception encountered: {}'.format(func.__name__, kwarg_str,
                                                                                           last_exception))

            else:
                raise RuntimeError('Max number of retries reached without success for '
                                   'method: {}({}). Last exception encountered: {}'.format(func.__name__, kwarg_str,
                                                                                           last_exception))
        return wrap_f
    return wrap
