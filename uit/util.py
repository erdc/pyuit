"""
********************************************************************************
* Name: util.py
* Author: nswain
* Created On: November 30, 2018
* Copyright: (c) Aquaveo 2018
********************************************************************************
"""
from functools import wraps
from time import sleep
from .exceptions import DpRouteError


def robust(retries=5):
    """
    Robust wrapper for client methods. Will retry, reties times if failed due to DP routing error.
    """  # noqa: E501
    def wrap(func):
        @wraps(func)
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
                sleep(1)
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


class HpcEnv:
    """
    A dictionary-like object that stores environmental variables from an HPC system.
    """

    def __init__(self, client):
        self.client = client
        self._env = dict()

    def __getitem__(self, item):
        return self.get(item)

    def __getattr__(self, item):
        return self.get(item)

    def __str__(self):
        return self._env.__str__()

    def __repr__(self):
        return self._env.__repr__()

    def get(self, item, default=None):
        if self._env.get(item) is None:
            self._env[item] = self.get_environmental_variable(item)
        return self._env.get(item) or default

    def get_environmental_variable(self, env_var_name, update=False):
        if not self.client.connected:
            raise RuntimeError('Must connect to system before accessing environmental variables.')

        if update or self._env.get(env_var_name) is None:
            self._env[env_var_name] = self.client.call(
                command=f'echo ${env_var_name}',
                working_dir='.',
            ).strip() or None

        return self._env.get(env_var_name)
