'''

'''

from  ambry.bundle.loader import ExcelBuildBundle
 
class Bundle(ExcelBuildBundle):
    ''' '''

    @staticmethod
    def nonnumber(v):
        # The OSHPD ID is a float, but should be a varchar, so this will get rid of the '.0' at the end
        try:
            return str(int(v))
        except ValueError:
            return v

    @staticmethod
    def excel_date(v):
        pass # We'll be overwriting this func when the workbook datemode is known. 

    def build_modify_row(self, row_gen, p, source, row):
        
        row['year'] = int(source.time)
        

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
        
    
    def row_gen_for_source(self, source_name):
        """ Total hack to set the datemode for excel date conversion. """
        
        rg = super(Bundle, self).row_gen_for_source(source_name)
        

        self.excel_date = rg.make_date_caster()
        
        return rg
        
        
        
        
        
