'''
This is basically the same as in psi. 
'''

from  ambry.bundle.loader import CsvBundle
from ambry.util import memoize


class Bundle(CsvBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def mangle_column_name(self, n):
        import re
        
        n = re.sub('[\r\n]+',' ',n)

        m = re.match(r'^PSI \#(\d+).*?(\w+)$', n)
        
        if not m:
            return n.lower()
            
        else:
            psi_no = m.group(1)
            ind_type = m.group(2).lower()
            return "psi_{}_{}".format(psi_no, ind_type)

    def counties_map(self):
        counties = { r['name'].replace(" County, California",'').lower(): r['gvid'] 
                     for r in  self.library.dep('counties').partition.rows  if int(r['state'] == 6)}

  
        return counties


    def meta(self):
        """Fix intitition of types."""
        
        r = super(Bundle, self).meta()
        
        with self.session:
            for t in self.schema.tables:
                for c in t.columns:
                    if c.name.endswith('rate'):
                        c.datatype = 'real'
                        c.data['caster'] = 'real_caster'
                    elif c.name.endswith('cases') or c.name.endswith('population'):
                        c.datatype = 'integer'
                        c.data['caster'] = 'int_caster'
        
            self.schema.add_column(t, 'gvid', datatype='varchar', fk_vid = 'c03x04003', indexes='i1,i2',
                                   description='GVid reference to county') 
        
        self.schema.write_schema()
        
        return r
        
    def build(self):

        for source in self.metadata.sources:

            p = self.partitions.find_or_new(table=source)

            p.clean()

            self.log("Loading source '{}' into partition '{}'".format(source, p.identity.name))

            lr = self.init_log_rate(print_rate = 5)

            header = [c.name for c in p.table.columns]

            counties_map = self.counties_map()

            with p.inserter() as ins:
               for _, row in self.gen_rows(source):
                   lr(str(p.identity.name))

                   d = dict(zip(header, row))
                   
                   d['gvid'] = counties_map.get(d['county'].lower(),None)
                   
                   e = ins.insert(d)

        return True
        
    def test(self):
        
        p = self.partitions.all.pop()
        
        assert p.stats.id.count == 420
        
        assert p.stats.gvid.nuniques == 58
        
        assert 'Fresno' in p.stats.county.uvalues.keys()
        