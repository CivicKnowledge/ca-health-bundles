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
        
    @property
    @memoize
    def facilities_map(self):

        return { r['facility_name'].lower(): dict(r) 
                 for r in  self.library.dep('facility_info').partition.rows if r['facility_name']}
        
    @property
    @memoize
    def hospital_names_by_county(self):
        from collections import defaultdict
        
        d = defaultdict(set)
        
        for r in  self.library.dep('facility_info').partition.rows:
            if r['facility_name']:
                d[r['county_gvid']].add(r['facility_name'].lower())

        return d


    def build_modify_row(self, row_gen, p, source, row):
        from difflib import get_close_matches
        
        row['year'] = int(source.time)
        #row['hospital'] = row['hospital'].decode('latin1')
        
        if row['county']:
            row['county_gvid'] = self.county_map[row['county'].lower()]
        else:
            row['county_gvid'] = None

        hn = row['hospital'].lower()
        
        if hn in self.facilities_map:
            row['oshpd_id'] = self.facilities_map[hn]['oshpd_id']
           
        elif row['county_gvid']:
            matches =  get_close_matches(hn,self.hospital_names_by_county[row['county_gvid']])
            
            if matches:
                row['matched_hospital_name'] = matches[0].title()
                row['oshpd_id'] = self.facilities_map[matches[0]]['oshpd_id']
            else:
                self.warn("Failed to get OSHPD_ID for "+hn)
        
        else:
            self.warn("Failed to get OSHPD_ID for "+hn)
        
  
        
    
        