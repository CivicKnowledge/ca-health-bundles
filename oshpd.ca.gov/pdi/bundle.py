'''
This is basically the same as in psi. 
'''

from  ambry.bundle.loader import CsvBundle
from ambry.util import memoize


class Bundle(CsvBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    @property
    @memoize
    def counties_map(self):
        counties = { r['name'].replace(" County, California",'').lower(): r['gvid'] 
                     for r in  self.library.dep('counties').partition.rows  if int(r['state'] == 6)}

        return counties
    

    def mangle_column_name(self, i, n):
        import re
        
        n = re.sub('[\r\n]+',' ',n)

        m = re.match(r'^(P.I) \#(\d+)(.*)$', n)
        
        if not m:
            mangled =  n.lower()
            
        else:
            grp = m.group(1).lower()
            psi_no = m.group(2)
            ind_type = re.sub(r'[\W]+', '_', m.group(3).strip().lower())
            mangled =  "{}_{}_{}".format(grp, psi_no, ind_type)

        if mangled in self.col_map and self.col_map[mangled]['col'] :
            return self.col_map[mangled]['col']
        else:
            return mangled



    def build_modify_row(self, row_gen, p, source, row):


            try:
                row['gvid'] =  self.counties_map[row['county'].lower()]
            except KeyError:
                pass
            
            



        
    def test(self):
        
        p = self.partitions.all.pop()
        
        assert p.stats.id.count == 420
        
        assert p.stats.gvid.nuniques == 58
        
        assert 'Fresno' in p.stats.county.uvalues.keys()
        