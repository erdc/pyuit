"""
********************************************************************************
* Name: util.py
* Author: nswain
* Created On: November 30, 2018
* Copyright: (c) Aquaveo 2018
********************************************************************************
"""

from functools import wraps
import logging
from time import sleep
import aiohttp
import requests  # Don't import only the exception because it conflicts with Python's standard ConnectionError
from .exceptions import MaxRetriesError

logger = logging.getLogger(__name__)


def robust(retries=1):
    """Robust wrapper for client methods. Will retry "retries" times if failed due to specific errors.

    This defaults to 1 retry because UIT+ should repair the SSH Tunnel immediately for a DP Route error.
    """

    def wrap(func):
        @wraps(func)
        def wrap_f(*args, **kwargs):
            attempts = 0

            last_exception = None

            while attempts <= retries:
                try:
                    return func(*args, **kwargs)
                except (RuntimeError, requests.exceptions.ConnectionError, aiohttp.client_exceptions.ServerDisconnectedError) as e:
                    if isinstance(e, RuntimeError) and "DP Route error" in str(e):
                        # "DP Route error" indicates failure of SSH Tunnel client on UIT Plus server.
                        # Successive calls should work.
                        error_text = "DP Route error"
                    elif isinstance(
                        e, requests.exceptions.ConnectionError
                    ) and "Connection aborted" in str(e):
                        # Requests very rarely end early with this "aborted" error.
                        error_text = "Connection aborted"
                    elif isinstance(e, aiohttp.client_exceptions.ServerDisconnectedError) and "Server disconnected" in str(e):
                        # Requests very rarely end early with this "aborted" error.
                        error_text = "Connection aborted"
                    else:
                        # Raise other RuntimeErrors and ConnectionErrors
                        raise

                    if attempts < retries:
                        logger.info(
                            f"'{error_text}' detected, @robust() is retrying {retries - attempts} more time(s)."
                        )
                        sleep(1)
                    attempts += 1
                    last_exception = e

            kwarg_str = ", ".join(['{}="{}"'.format(k, v) for k, v in kwargs.items()])
            raise MaxRetriesError(
                f"Max number of retries reached without success for method: {func.__name__}({kwarg_str}). "
                f"Last exception encountered: {last_exception}"
            )

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
            raise RuntimeError(
                "Must connect to system before accessing environmental variables."
            )

        if update or self._env.get(env_var_name) is None:
            self._env[env_var_name] = (
                self.client.call(
                    command=f"echo ${env_var_name}",
                    working_dir=".",
                ).strip()
                or None  # noqa: W503
            )

        return self._env.get(env_var_name)


class AsyncHpcEnv(HpcEnv):
    def get(self, item, default=None):
        if self._env.get(item) is None:
            raise AttributeError(
                f'The variable "{item}" has not yet been retreived. '
                f'You must first await an asychronous call to `get_environment_variable("{item}")` to retreive the '
                f"variables value."
            )
        return self._env.get(item) or default

    async def get_environmental_variable(self, env_var_name, update=False):
        if not self.client.connected:
            raise RuntimeError(
                "Must connect to system before accessing environmental variables."
            )

        if update or self._env.get(env_var_name) is None:
            result = await self.client.call(
                command=f"echo ${env_var_name}",
                working_dir=".",
            )
            self._env[env_var_name] = result.strip() or None

        return self._env.get(env_var_name)
