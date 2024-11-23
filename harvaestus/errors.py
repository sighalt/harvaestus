

class HarvaestusError(Exception):
    pass


class FixableError(HarvaestusError):

    def __init__(self, error_key, **kwargs):
        self.error_key = error_key
        self.data = kwargs

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.error_key == other.error_key

        return super().__eq__(other)


class IgnoreKey(HarvaestusError):
    pass


class BacklogError(HarvaestusError):
    pass


class EmptyBacklog(BacklogError):
    pass


class ReAddLimitReached(BacklogError):
    pass


class StorageNotAvailable(HarvaestusError):
    pass


class DataIsNotAllowed(HarvaestusError):
    pass
