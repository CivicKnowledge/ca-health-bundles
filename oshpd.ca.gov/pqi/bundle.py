'''

'''

from  ambry.bundle.loader import ExcelBuildBundle
 

class Bundle(ExcelBuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def meta(self):
    
        self.meta_schema()
        
        return True

    def gen_rows(self, as_dict=False):
        from xlrd import open_workbook
        import re
        from ambry.orm import Column
        
        fn, sheet_num = self.get_wb_sheet('pqi')

        wb = open_workbook(fn)
         
        s = wb.sheets()[sheet_num]
        
        rx = re.compile(r'(PQI \#\d+)')
        
        pqis = []
        last_group = None
        header = None
        for i  in range(1,s.nrows):
            
            if i < 4:
                continue
                
            row = self.srow_to_list(i, s)
                
            if i == 4:
                
                for j,c in enumerate(row):
                    groups = rx.match(c)
                    
                    if groups:
                        last_group = groups.group(1)
                        
                    pqis.append(last_group)
            elif i == 5:
                pass
            elif i == 6:
                header = [ ' '.join(str(k).replace('\n',' ') if k else '' for k in x).strip() for x in zip(pqis,row)]
                header[0] = 'year'
                header[1] = 'county'
                header = ['id'] + header
                header = [ Column.mangle_name(x) for x in header ]
            else:

                if as_dict:
                    yield dict(zip(header, [None]+row))
                else:
                    yield header, [None]+row

    def meta_schema(self):
        from xlrd import open_workbook
        import re
        
        header, _ = self.gen_rows().next()
                
        self.prepare()
        
        with self.session:
            t = self.schema.add_table('pqi', description = 'Prevention Quality Indicators')
            self.schema.add_column(t, 'id', datatype = 'INTEGER', is_primary_key = True)
            
            for col in ['id']+header:
                self.schema.add_column(t, col, datatype = 'INTEGER', description = col)
                
                
        self.schema.write_schema()
        
        return True
        
    def meta_update(self):
     
        self.prepare()

        self.schema.update_from_iterator('pqi', 
                                   header = self.gen_rows().next()[0],
                                   iterator=self.gen_rows(),
                                   logger=self.init_log_rate(print_rate=10))

    def build(self):
        from xlrd import open_workbook
        import re
        
        p = self.partitions.find_or_new(table = 'pqi')
        p.clean()
        
        counties = { r['name'].replace(" County, California",'').lower(): r['gvid'] 
                     for r in  self.library.dep('counties').partition.rows  if int(r['state'] == 6)}
        
        with p.inserter() as ins:
            for row in self.gen_rows(as_dict=True):
                
                row = dict(row)
                
                row['gvid'] = counties.get(row['county'].lower(),None)
                
                e = ins.insert(row)
                if e:
                    self.error(e)
                
        return True
                


