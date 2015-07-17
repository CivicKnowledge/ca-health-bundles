""""""

from ambry.bundle import BuildBundle


class Bundle(BuildBundle):

    """"""

    def __init__(self, directory=None):

        super(Bundle, self).__init__(directory)

    def download(self):
        
        for source in self.sources():
            print source

    def build(self):

        return True
