
from  ambry.bundle.loader import GeoBuildBundle

class Bundle(GeoBuildBundle):
    ''' '''
    pass
    

    def test_partitions(self):
        
        for p in self.partitions.all:
            print p.data['source_data']


