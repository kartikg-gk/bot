"""Failures raised while talking to a Delta RPC process."""


class WorkerLost(RuntimeError):
    pass


class RequestRejected(RuntimeError):
    def __init__(self, error):
        self.code = error["code"]
        super().__init__(error["message"])
