""""""

from ambry.bundle.loader import LoaderBundle
from ambry.bundle.rowgen import GeneratorRowGenerator


class Bundle(LoaderBundle):

    """"""

    def meta_schema(self):
        """Create the table from the row generator"""
        self.meta_intuit_table('facilities_index', self.row_generator)
        self.schema.write_schema()
        
    @property
    def row_generator(self):
        """A row generator function that joins two library tables"""
        f = self.library.dep('facilities').partition
        g = self.library.dep('geoids').partition
        
        atch_name = f.attach(g)
        
        # There are multiple entries for each facility, so take the most recent
        q = """SELECT * FROM facilities AS f 
        LEFT JOIN {}.facilities_geoids  AS fg ON f.id = fg.facilities_id 
        group by oshpd_id order by year desc
        """.format(atch_name)
        
        row_proxy = f.query(q)
        
        def rrg():

            for i, row in enumerate(row_proxy):
                
                if i == 0:
                    yield row.keys()
                
                yield row
   
        return GeneratorRowGenerator(rrg)
   
   
    def build_modify_row(self, row_gen, p, source, row):

        row['type_code'], row['oshpd_short_id'] = row['oshpd_id'][:3], row['oshpd_id'][3:]

        del row['id']
        
    def build(self):

        p = self.partitions.find_or_new(table='facilities_index')
        p.clean()
        
        self.build_from_row_gen(self.row_generator, p)

        return True
        

        
        
