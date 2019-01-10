"""Custom Exceptions."""


class DpRouteError(RuntimeError):
    """
    A custom exception for DP_ROUTE class errors.

    When making calls through UIT, the SSH tunnels to the supercomputers are very flaky. Most of the time a few retries
    will suffice, but occasionally not. In certain cases, such as status updates, it is advantageous to ignore the
    errors, while in others it is best to just notify the users. Thus, we created this specific exception to identify
    this case.
    """
    def __init__(self, message):
        super(message)
