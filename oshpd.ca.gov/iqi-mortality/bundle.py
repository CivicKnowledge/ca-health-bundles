'''

'''



from  ambry.bundle.loader import ExcelBuildBundle
from ambry.bundle.rowgen import RowSpecIntuiter
from ambry.util import memoize
class RowIntuiter(RowSpecIntuiter):
    """This RowIntuiter is defined specifically for the files in this bundle, making a few adjustments to 
    detect lines that are not data rows. """
    
    def is_data_line(self, i, row):
        """Return true if a line is a data row"""
      
        return not self.is_header_line(i,row) and not self.is_header_comment_line(i,row)

    def is_header_line(self, i, row):
        """Return true if a row is part of the header"""
        
        return row[0] == 'COUNTY'
        
        
    def is_header_comment_line(self, i, row):
        """Return true if a line is a header comment"""
        return len(filter(bool, row)) < 2
        

class Bundle(ExcelBuildBundle):
    ''' '''

    prefix_headers = ['id','year','county_gvid','facility_index_id']

    @staticmethod
    def  int_caster(v):
        """Remove commas from numbers"""
        
        
        v = str(v).replace('.','').replace(',','')
        
        if not bool(v):
            return None
        
        try:
            return int(v)
        except AttributeError:
            return v

    @staticmethod
    def  real_caster(v):
        """Remove commas from numbers"""
        
        v = str(v).replace('.','').replace(',','')

        if not bool(v):
            return None

        try:
            return float(v)
        except AttributeError:
            return v
    


   
    def meta_set_row_specs(self):
        
        super(Bundle,self).meta_set_row_specs(RowIntuiter)
        
    def meta_fix_datatypes(self):
        
        for t in self.schema.tables:
            for c in t.columns:
                if c.name.endswith('rate'):
                    c.datatype = 'real'
                    c.data['caster'] = 'real_caster'
                elif c.name.endswith('cases') or c.name.endswith('deaths') :
                    c.datatype = 'integer'
                    c.data['caster'] = 'int_caster'
                elif  c.name.endswith('ratings') :
                    c.datatype = 'varchar'
                    c.data['caster'] = None
                elif c.name.endswith('gvid'):
                    c.proto_vid = 'c00104002'
                    c.fk_vid = 'c03x04003'
                elif c.name == 'year':
                    c.proto_vid = 'c00102003'
                    c.fk_vid = 'c03x04002'
                    
                    
        self.schema.write_schema()
        
    @property
    @memoize
    def county_map(self):
        return { r['name'].replace(" County, California",'').lower(): r['gvid'] 
                     for r in  self.library.dep('counties').partition.rows  if int(r['state'] == 6)}
        
    def build_modify_row(self, row_gen, p, source, row):
        
        row['year'] = int(source.time)
        #row['hospital'] = row['hospital'].decode('latin1')
        
        if row['county']:
            row['county_gvid'] = self.county_map[row['county'].lower()]
        

        
        
        

        
    
        