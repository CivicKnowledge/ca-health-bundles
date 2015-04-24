'''

'''

from  ambry.bundle.loader import ExcelBuildBundle
 
class Bundle(ExcelBuildBundle):
    ''' '''

    def meta(self):
        """Fix some pathalogical datatypes that the intuiter gets wrong. """
        r = super(Bundle, self).meta()
        
        self.prepare()
        
        with self.session:
            for t in self.schema.tables:
                for c in t.columns:
                    
                    if 'acqui_means' in c.name:
                        c.datatype = 'varchar'
                        
                    if 'acqui_dt' in c.name or '_value_' in c.name or 'projtd' in c.name:
                        c.datatype = 'integer'
                    
                    
        self.schema.write_schema()
        
        return r
        
    def tp(self):
        
        for p in self.partitions.all:
            
            p.write_full_stats()

