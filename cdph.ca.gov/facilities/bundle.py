'''

'''

from  ambry.bundle.loader import CsvBundle
 


class Bundle(CsvBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

        self.counties = { r['name'].replace(" County, California",'').lower(): r['gvid'] 
                     for r in  self.library.dep('counties').partition.rows  if int(r['state'] == 6)}

        
        from datetime import datetime
        self.year = datetime.now().year
        


    def build_modify_row(self, row_gen, p, source, row):
        """Called for every row to allow subclasses to modify rows. """
        from geoid.civick import GVid, Zip
        
        row['name'] = row['name'].decode('latin1')
        row['county_gvid']  = self.counties.get(row['county'].lower())
        row['year'] = self.year
        if row['zip'] :
            cg = GVid.parse(row['county_gvid'])
            if cg:
                zg = Zip(int(row['zip']))
                row['zip_gvid']   = str(zg)
               

    
        


