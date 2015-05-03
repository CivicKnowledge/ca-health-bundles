""""""

from ambry.bundle.loader import LoaderBundle


class Bundle(LoaderBundle):

    """"""


    def __init__(self, directory=None):
        super(Bundle, self).__init__(directory)

        
    def build(self):
        
        super(Bundle, self).build()
        
        self.build_codes()
        
        return True
        
    def build_modify_row(self, row_gen, p, source, row):

        # Can't seem to figure out how to get this character encoded correctly
        row['msdrg_name'] = row['msdrg_name'].replace('\xad','-')
       
    def codes_map(self):
        
        code_map = {}
        
        t = self.schema.table('pdd_puf')

        for c in t.columns:
            rcm = c.reverse_code_map
            if rcm:
                code_map[c.name] = rcm

        return code_map

    
    def build_codes(self):
        
        p_in = self.partitions.find(table = 'pdd_puf')
        p_out = self.partitions.find_or_new(table = 'pdd_puf_c')
        p_out.clean()
        
        cm = self.codes_map()

        def sub(k,v):
            
            try:
                return str(cm[k][v])
            except KeyError:
                return v
            
        lr = self.init_log_rate(1000)
        
        with p_out.inserter() as ins:
            
            for row in p_in.rows:
                nr = { k:sub(k,v) for k,v in row.items() }
                
                e = ins.insert(nr)
                
                lr("Build codes")
            
                if e:
                    self.error(e)
            
    
    def meta_collect_codes(self):
        
        with self.session:
            
            t = self.schema.table('pdd_puf')
            p = self.partitions.find(table = 'pdd_puf')
            
            for c in t.columns:
                
                values = []
                
                if c.type_is_text() and c.size > 10:
                    
                    self.log("Getting values for '{}' ".format(c.name))
                    
                    q = "SELECT DISTINCT {} FROM pdd_puf".format(c.name)
                    
                    for row in  p.query(q):
                        values.append(row[0])
                        
                    if len(values) > 50:
                        self.error("Too Many Values")
                        continue
                    
                    for i,k in enumerate(values):
                        c.add_code(k,str(i))

                
        self.schema.write_codes()
    
       
    
        
    def meta_expand_sources(self):
        
        files = [
            ('la', 'LA_lbl.txt'),
            ('northa', 'Northa_lbl.txt'),
            ('northb', 'Northb_lbl.txt'),
            ('south', 'South_lbl.txt') ]
            
        for source_name, source in self.metadata.sources.items():
            
            for suffix, regex in files:
                d = dict(source.items())
                d['filetype'] = 'csv'
                d['table'] = 'pdd_puf'
                d['file'] = regex
                
                name = source_name + '_' + suffix
                
                self.metadata.sources[name] = d
                
        self.metadata.write_to_dir()
        
    def meta_code_schema(self):
        
        self.prepare()
        
        cm =  self.codes_map()
        
        with self.session:
        
            t = self.schema.table('pdd_puf_c')
        
            for c in t.columns:
                if c.name in cm:
                    c.datatype = 'INTEGER'
                    
        self.schema.write_schema()
        
    def get_code_labels(self):
        
        import csv
        
        cols = set()
        with open("meta/codes.csv") as f:
            r = csv.DictReader(f)
            for row in r:
                cols.add(row['column'])
            
        last_col = 'None'    
        with open('rawcodes.txt') as f:
            for line in f:
                line = line.strip()
                
                if line in cols:
                    last_col = line
                    
                else:
                    print last_col, line
                    
    def test_wfs(self):
        
        p = self.partitions.find(table = 'pdd_puf')
        
        
        
        p.write_full_stats()
                    
                
                
            
            
        
        
            
            
        
