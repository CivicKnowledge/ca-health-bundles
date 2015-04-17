'''

'''

from  ambry.bundle.loader import CsvBundle
from ambry.util import memoize


class Bundle(CsvBundle):
    ''' '''



    @staticmethod
    def latin_decode(v):
        from unidecode import unidecode
        
        return unidecode(v.decode('latin1'))

    def set_desc(self):
        import re
        
        for k, v in self.metadata.sources.items():
         
            g =  re.match(r'(\d+)(\w+)', k).groups()
            
            d = { 'Vol': 'Volume', 'Util': 'Utilization'}
            
            self.metadata.sources.get(k).description = '{} {}'.format(g[0], d[g[1]])

        self.metadata.write_to_dir(write_all=True)


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
        self.build_import()
        self.build_summary()
        
        return True
        
    def build_import(self):
        import re
        
        lr = self.init_log_rate()
        
        counties_map = self.counties_map()
        
        def do_insert(table, year, k):
            
            p = self.partitions.find_or_new(table=table, grain = None)
            
            header = [ c.name for c in p.table.columns]
            
            with p.inserter() as ins:
                for _, row in self.gen_rows(k):

                    drow = dict(zip(header, [None]*3 + row ))
              
                    drow['year'] = year
                   
                    lr("{} {}".format(table, year))
                    
                    drow['gvid'] = counties_map.get(drow['county'], None)
                    

                    if drow['gvid'] and drow['hospital_name']:
                        match =  self.match_name(drow['gvid'], drow['hospital_name'].decode("latin1"))
                        
                        if match:
                            score, name, ids = match
                            drow['facility_index_id'] = ids[0]
                    
                    e = ins.insert(drow)
                    
                    if e:
                        self.error("Insert error {}".format(e))
                    
        for k, v in self.metadata.sources.items():
            if k.endswith('Util'):
                year = int(k.replace('Util',''))
                do_insert('utilization',year, k)
                         
            elif k.endswith('Vol'):
                year = int(k.replace('Vol',''))
                do_insert('volume',year, k)
                
        return True
        
    def build_summary(self):
        
        for table_name in ('utilization', "volume"):

            p = self.partitions.find_or_new(table=table_name, grain='county')
            p.clean()
        
            self.log("Building {}".format(p.identity))
        
            df = self.partitions.find_or_new(table=table_name, grain = None).pandas
        
            df[(df.hospital_name != 'STATEWIDE TOTAL')].groupby(['year','county', 'gvid']).sum().reset_index().drop('id',axis=1)
        
            with p.inserter() as ins:
                for row in df.to_dict(orient='records'):
                    
                    if row['gvid']:
                        ins.insert(row)
     
    @memoize
    def name_link_dicts(self):
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
            

            
            
        
    
        

          