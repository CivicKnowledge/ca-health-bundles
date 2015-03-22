'''

'''

from  ambry.bundle.loader import CsvBundle
 


class Bundle(CsvBundle):
    ''' '''

    @staticmethod
    def  int_caster(v):
        """Remove commas from numbers"""
        
        try:
            return int(v.replace(',',''))
        except AttributeError:
            return v

    @staticmethod
    def  real_caster(v):
        """Remove commas from numbers"""
        
        try:
            return float(v.replace(',',''))
        except AttributeError:
            return v
    

    def set_desc(self):
        import re
        
        for k, v in self.metadata.sources.items():
         
            g =  re.match(r'(\d+)(\w+)', k).groups()
            
            d = { 'Vol': 'Volume', 'Util': 'Utilization'}
            
            self.metadata.sources.get(k).description = '{} {}'.format(g[0], d[g[1]])

        self.metadata.write_to_dir(write_all=True)

    def gen_rows(self, source=None, as_dict=False):
        
        return super(Bundle, self).gen_rows(source, as_dict,  
                                            prefix_headers = ['id','year','gvid','facility_index_id'])
        

    def meta(self):
        
        self.prepare()
        self.meta_schema()
        self.meta_intuit()
        
        return True
        
    def meta_schema(self):
        import csv
        from ambry.orm import Column
        import re
        
        
        combined_header = {}
        
        for k in self.metadata.sources:
            fn = self.get_source(k)
            
            table, year = k.split('_')
            
            if table not in combined_header:
                combined_header[table] = []
            
            with open(fn) as f:
                r = csv.reader(f)
                header = r.next()
                
                for c in header:
                    if c not in combined_header[table]:
                      
                        combined_header[table].append(c)
                        

        with self.session:
            
            for k, v in  combined_header.items():
                t = self.schema.add_table(k)
                self.schema.add_column(t, 'id', datatype='integer', is_primary_key=True)
                self.schema.add_column(t, 'year', datatype='integer', fk_vid = 'c03x04002', indexes='i2') 
                self.schema.add_column(t, 'gvid', datatype='varchar', fk_vid = 'c03x04003', indexes='i1,i2') 
                self.schema.add_column(t, 'facility_index_id', datatype='integer', fk_vid = 't03A01', indexes='i3')
                for i,c in enumerate(v):
                    
                    if not c:
                        c = "blank{}".format(i)
                    
                    caster = None
                    
                    if 'Rate' in c:
                        datatype = 'real'
                        caster = 'real_caster'
                    elif 'Ratings' in c or Column.mangle_name(c) in ('unty','hospital','comment_letters'):
                        datatype = 'varchar'
                    else:
                        datatype = 'integer'
                        caster = 'int_caster'
                  
                    self.schema.add_column(t, Column.mangle_name(c), datatype=datatype,
                                            description = re.sub('[\r\n]+',' ', c), data = {'caster': caster})
                    
        self.schema.write_schema()
     
    def meta_intuit(self):
        
        with self.session:
            for source_name, source in self.metadata.sources.items():

                table_name = source.table if source.table else  source_name

                table_name, year = table_name.split('_')

                table_desc = source.description if source.description else "Table generated from {}".format(source.url)

                data = dict(source)
                del data['description']
                del data['url']


                table = self.schema.add_table(table_name, description=table_desc, data = data)

                header, row = self.gen_rows(source_name, as_dict=False).next()

                header = [ x for x in header if x]

                def itr():
                    for header, row in self.gen_rows(source_name, as_dict=False):
                        yield row

                self.schema.update_from_iterator(table_name,
                                   header = header,
                                   iterator=itr(),
                                   max_n=1000,
                                   logger=self.init_log_rate(500))
                                   
                                   
    def build(self):
        from ambry.orm import Column

        for source in self.metadata.sources:

            table_name, year = source.split('_')

            p = self.partitions.find_or_new(table=table_name)
            
            p.clean()

        for source in self.metadata.sources:

            table_name, year = source.split('_')

            p = self.partitions.find_or_new(table=table_name)

            self.log("Loading source '{}' into partition '{}'".format(source, p.identity.name))

            lr = self.init_log_rate(print_rate = 5)

            header = [c.name for c in p.table.columns]

            with p.inserter() as ins:
               for row in self.gen_rows(source, as_dict = True):
                   lr(str(p.identity.name))

                   d = { Column.mangle_name(k):v for k,v in row.items() }

                   d['year'] = int(year)
                   d['hospital'] = d['hospital'].decode('latin1')
                   ins.insert(d)


        return True
        
        

        
    
        