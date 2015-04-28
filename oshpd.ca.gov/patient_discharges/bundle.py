""""""

from ambry.bundle.loader import LoaderBundle


class Bundle(LoaderBundle):

    """"""

    def __init__(self, directory=None):

        super(Bundle, self).__init__(directory)

    def build(self):

        return True
