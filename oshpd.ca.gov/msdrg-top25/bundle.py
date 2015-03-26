'''

'''

from  ambry.bundle.loader import ExcelBuildBundle
 


class Bundle(ExcelBuildBundle):
    ''' '''

    prefix_headers = ['id', 'year']


    def build_modify_row(self, row_gen, p, source, row):
        """Called for every row to allow subclasses to modify rows. """
        row['year'] = int(source.time)
        
        # The '106' indicates a hospital, which is part of the full facility id, but not included 
        # this database because all of the records are for hospitals
        
        row['facilitynumber'] = '106'+row['facilitynumber']
        
         