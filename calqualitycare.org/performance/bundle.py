""""""

from ambry.bundle.loader import ExcelBuildBundle


class Bundle(ExcelBuildBundle):

    """"""


    def __init__(self, directory=None):

        super(Bundle, self).__init__(directory)
        
    def build_modify_row(self, row_gen, p, source, row):


        row['year'] = int(source.time)
        
        # The oshpd_id appears to be a float in the excel spreadsheet, with gives it a
        # '.0' at the end when converted to a string 
        
        try:
            row['oshpd_id'] = str(int(row['oshpd_id']))
        except KeyError:
            row['oshpd_id'] = None
        

    def meta(self):
        """Repair the oshpd_id var, which the intuit process thinks is an integer"""
        super(Bundle, self).meta()
        
        for t in self.schema.tables:
            t.column('oshpd_id').datatype = 'varchar'
            
        self.schema.write_schema()
        
        return True
    
    
        


