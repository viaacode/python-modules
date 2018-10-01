
class Null:
    """ Null objects always and reliably "do nothing." """

    def __init__(self, *args, **kwargs):
        pass

    def __call__(self, *args, **kwargs):
        return self

    def __repr__(self):
        return "Null()"

    def __nonzero__(self):
        return False

    def __getattr__(self, name):
        return self

    def __setattr__(self, name, value):
        return self

    def __delattr__(self, name):
        return self

    def __wrapped__(self):
        raise AttributeError("Not supported")


# singleton
Null = Null()
