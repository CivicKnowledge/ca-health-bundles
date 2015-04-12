'''

'''

from  ambry.bundle.loader import ExcelBuildBundle
from ambry.util import memoize


class Bundle(ExcelBuildBundle):
    ''' '''

    prefix_headers = ['id','county_gvid','year']
    

    def __init__(self,directory=None):
        import os
        super(Bundle, self).__init__(directory)
        
        if os.path.exists(self.filesystem.path('meta/colmap.csv')):
            self.col_map =  self.filesystem.read_csv('meta/colmap.csv', 
                                                     key=lambda row: (row['header_name'], row['col_name']))
        
        
    def decode(self, x):
        try:
            return x.encode('utf8').decode('ascii','ignore')
        except:
            print x
            raise

    @property
    @memoize
    def county_map(self):
        return { r['name'].replace(" County, California",'').lower(): r['gvid'] 
                     for r in  self.library.dep('counties').partition.rows  if int(r['state'] == 6)}
    

    @property
    @memoize
    def column_map(self):
        cm =  self.filesystem.read_csv('meta/colmap.csv', key=lambda row: (row['header_name'], row['col_name']))    
                 
        cm['id'] = 'id'
        cm['year'] = 'year'
        cm['county_gvid'] =  'county_gvid'
                                        
        return cm
                                                 
                                                 
                                                                                   
    def meta_set_segment(self):
        """Many of the Excel files have the data on a sheet that isn't the first. """
        from xlrd import open_workbook
        
        for source_name, fn in self.sources():
            wb = open_workbook(fn)
            sheets = wb.sheets()
            print source_name
            
            rows = [ s.nrows for s in sheets]
            
            seg =  rows.index(max(rows))
         
            self.metadata.sources[source_name].segment = seg
            
            lf = dict(
                is_header_comment_line = lambda row: len(filter(bool, row)) > 0 and len(filter(bool, row)) <= 3,
                is_header_line = lambda row: len(filter(bool, row)) > 3
            )
        
            if 'child_care' in source_name:
                lf['is_data_line'] = lambda row:self.test_type(int, row[5]) 
            else:
                lf['is_data_line'] = lambda row:self.test_type(int, row[0]) 
            
            #self.metadata.sources[source_name].row_spec =  {}
            
            self.metadata.sources[source_name].row_spec = self.intuit_row_spec(source_name, **lf)
        
        self.metadata.write_to_dir()
        
        
    def meta_check_rows(self):
        """Create a CV file with all of the headers from the sources. """
        import os
        import csv
        
        headers_fn  = self.filesystem.path('meta','headers.csv')
        
        colmap_fn  = self.filesystem.path('meta','colmap.csv')
        
        cols = {}
        
        if os.path.exists(colmap_fn):
            with open(colmap_fn) as f:
                
                for row in csv.reader(f):
                    if len(row) == 2:
                        cols[row[0]] = (row[1], None)
                    if len(row) == 3:
                        cols[row[0]] = (row[1], row[2])
                
        
        with open(headers_fn, 'w') as f:
            w = csv.writer(f)
            
            w.writerow('years kind header'.split(' '))
        
            for source_name, fn in self.sources():
            
                self.log(source_name)
            
                for i, (header, row) in enumerate(self.gen_rows(source_name)):
                     if i > 10:
                         break    
                     
                row = source_name.split('_') + header
          
                w.writerow(row)
                
                for c in header:
                    if c not in cols:
                        cols[c] = None
        
        with open(colmap_fn, 'w') as f:
            w = csv.writer(f)
            for k in sorted(cols.keys()):
                w.writerow([k, cols[k][0], cols[k][1]])
            

                
    def mangle_column_name(self, i, name):
        
        try:
            return self.column_map[name]
        except KeyError:
            self.error("Failed to get '{}' from map. {} ".format(name, self.column_map.keys()))
            raise 
            
    def build_modify_row(self, row_gen, p, source, row):
        """Called for every row to allow subclasses to modify rows. """
        
        row['year'] = source.time
        try:
            row['county_gvid'] = self.county_map.get(row['county'].lower())
        except:
            print self.error(row)
            raise 
            
    def xbuild(self):

        lr = self.init_log_rate()

        for p in self.partitions:
            p.clean()
        
        for source_name, source in self.metadata.sources.items():
        

            with p.inserter() as ins:
                for i, (header, row) in enumerate(self.gen_rows(source_name)):

                    d = dict(zip(header, row))
                    
                    d['year'] = first_year
                    d['county_gvid'] = counties.get(d['county'].lower())
               
                   
                    lr(str(p.identity.name))
                  
                    e = ins.insert(d)
                 
                    
                    if e:
                        self.error("Insert Error: {} ".format(e))
                
        return True
            