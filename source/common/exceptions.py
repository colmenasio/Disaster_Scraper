class InformationFetchingError(IOError):
    def __init__(self, inner_exception: Exception | None = None, message: str = ""):
        # Call the base class constructor with the parameters it needs
        self.message = message
        super().__init__(message)
        self.inner_exception = inner_exception


class IPBanError(IOError):
    def __init__(self, *args):
        super().__init__(*args)