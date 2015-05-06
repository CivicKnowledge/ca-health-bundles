""""""

from ambry.bundle import BuildBundle


class Bundle(BuildBundle):

    """"""

    def __init__(self, directory=None):

        super(Bundle, self).__init__(directory)

    def meta_copy_schema(self):
        pass

    def build(self):

        return True
        
    def t(self):
        
        p = self.library.dep('pddpuf').partition
        
        q = "SELECT id, oshpd_id, 'year', msdrg FROM pdd_puf_c LIMIT 200000"
        
        df = p.select(q,index_col=p.get_table().primary_key.name).pandas

        print df.groupby(['oshpd_id','msdrg','year']).count()