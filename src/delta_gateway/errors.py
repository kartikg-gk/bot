"""Failures raised while talking to a Delta RPC process."""


class WorkerLost(RuntimeError):
    pass


class RequestRejected(RuntimeError):
    def __init__(self, error):
        self.code = error["code"]
        super().__init__(error["message"])


class SetupError(RuntimeError):
    """A bot's session could not start because of its configuration, such as a missing key.

    The message names what to fix and holds no secret, so it is shown to the person.
    """
