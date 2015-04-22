'''

'''

from  ambry.bundle.loader import ExcelBuildBundle
from ambry.util import memoize


class Bundle(ExcelBuildBundle):
    ''' '''

    @staticmethod
    def latin_decode(v):
        from unidecode import unidecode
        
        return unidecode(v.decode('latin1'))

    @memoize
    def counties_map(self):
        counties = { r['name'].replace(" County, California",''): r['gvid'] 
                     for r in  self.library.dep('counties').partition.rows  if int(r['state'] == 6)}

        return counties
  
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
        
    def build_modify_row(self, row_gen, p, source, row):
        
        row['year'] = int(source.time)
        #row['hospital'] = row['hospital'].decode('latin1')
        
        if row['county']:
            row['county_gvid'] = self.counties_map[row['county'].lower()]
    

        
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
     
    @memoize
    def name_link_dicts(self):
        """Create a set of dictionaries that group facilities by county, for matching"""
        facilities = self.library.dep('facilities').partition
        
        fac_by_county = {}
        fac_all = []
        
        for row  in facilities.rows:
            if row['county_gvid'] not in fac_by_county:
                fac_by_county[row['county_gvid']] = []
            
            ids = (row['id'], row['oshpd_id'], row['cdph_id'])
            
            if row['oshpd_name']:
                fac_by_county[row['county_gvid']].append((row['oshpd_name'], ids))
                fac_all.append((row['oshpd_name'], ids))
            
            if row['cdph_name']:
                fac_by_county[row['county_gvid']].append((row['cdph_name'], ids))
                fac_all.append((row['cdph_name'], ids))
        
        return fac_by_county, fac_all
        
    def match_name(self, county, n):
        """Match hospital names by name and county"""
        from fuzzywuzzy import fuzz
        scores = []
        
        fac_by_county, fac_all = self.name_link_dicts()
         
        cfac = fac_by_county[county]
        
        for e in cfac:

            score = fuzz.ratio(n, e[0])
        
            if score >= 80:
                scores.append( (score, e[0], e[1]))

        if scores:
            scores = sorted(scores, key = lambda x : x[0])
            return scores[0]
            
        else:
            return None
                  