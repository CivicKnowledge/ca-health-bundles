'''

'''

from  ambry.bundle.loader import ExcelBuildBundle
from ambry.util import memoize


class Bundle(ExcelBuildBundle):
    ''' '''

    @staticmethod
    def latin_decode(v):
        """ A Caster for decoding hospital names"""
        from unidecode import unidecode
        
        return unidecode(v.decode('latin1'))

    def meta_combine_tables(self):
        
        self.prepare()
        
        with self.session:
            table = self.schema.add_table("utilization", description="Combined columns from all other utilization tables")
            self.schema.add_column(table, 'id', datatype='integer', is_primary_key=True)
            self.schema.add_column(table, 'year', datatype='integer')
            
            # This must be in the #sessions' with block. The copy_table() function also has a  session block
            # and the commit in that block will kick the table out of the session. Nesting prevents the commit
            # until this block exits. 
            for t in self.schema.tables:
                
                if t.name.endswith('util'):
                    self.schema.copy_table(t, 'utilization')
            
        with self.session:
            table = self.schema.add_table("volume", description="Combined columns from all other volume tables")
            self.schema.add_column(table, 'id', datatype='integer', is_primary_key=True)
            self.schema.add_column(table, 'year', datatype='integer')
            
            # This must be in the #sessions' with block. The copy_table() function also has a  session block
            # and the commit in that block will kick the table out of the session. Nesting prevents the commit
            # until this block exits. 
            for t in self.schema.tables:
                if t.name.endswith('vol'):
                    self.schema.copy_table(t, 'volume')                
                
        self.schema.write_schema()
        
    def build(self):
        super(Bundle, self).build()
        #self.build_summary()
        
        return True
        
    
    def build_summary(self):
        """Use Pandas to sum hospital records to counties."""
        for table_name in ('utilization', "volume"):

            p = self.partitions.find_or_new(table=table_name, grain='county')
            p.clean()
        
            self.log("Building {}".format(p.identity))
        
            df = self.partitions.find(table=table_name, grain = None).pandas
        
            df[(df.hospital_name != 'STATEWIDE TOTAL')].groupby(['year','county', 'gvid']).sum().reset_index().drop('id',axis=1)
        
            with p.inserter() as ins:
                for row in df.to_dict(orient='records'):
                    
                    if row['gvid']:
                        ins.insert(row)
                        
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

        hn = row['hospital_name'].lower()
        
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
                        
     

        

                  