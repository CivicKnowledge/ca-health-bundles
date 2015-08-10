""""""

from ambry.bundle.loader import ExcelBuildBundle
from ambry.util import memoize


class Bundle(ExcelBuildBundle):

    """"""

    @property
    @memoize
    def counties_map(self):
        counties = { r['name'].replace(" County, California",'').lower(): r['gvid']
                     for r in  self.library.dep('counties').partition.rows  if int(r['state'] == 6)}

        return counties


    def build_modify_row(self, row_gen, p, source, row):


            try:
                row['gvid'] =  self.counties_map[row['county_name'].lower()]
            except KeyError:
                pass