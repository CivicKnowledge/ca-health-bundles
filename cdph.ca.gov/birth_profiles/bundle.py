from ambry.bundle.loader import ExcelBuildBundle
from ambry.bundle.rowgen import RowSpecIntuiter

class RowIntuiter(RowSpecIntuiter):
    """This RowIntuiter is defined specifically for the files in this bundle, making a few adjustments to 
    detect lines that are not data rows. """
    
    def is_data_line(self, i, row):
        """Return true if a line is a data row"""
      
        try:
            float(str(row[0]).replace('*',''))
            return True
        except ValueError as e:
            
            return False

    def is_header_line(self, i, row):
        """Return true if a row is part of the header"""
        if self.is_header_comment_line(i, row):
            return False
            
        return not self.is_data_line(i, row)
        
        
    def is_header_comment_line(self, i, row):
        """Return true if a line is a header comment"""
        return len(filter(bool, row)) < 2

class Bundle(ExcelBuildBundle):
    ''' '''

    def __init__(self,directory=None):
        import os
        
        super(Bundle, self).__init__(directory)

        self.col_map_fn = self.filesystem.path('meta', 'column_map.csv')

        if os.path.exists(self.col_map_fn):
            self.col_map = self.filesystem.read_csv(self.col_map_fn, key = 'header')


    def meta_set_row_specs(self):
        
        for source_name in self.metadata.sources:
            source = self.metadata.sources.get(source_name)
           
            rg = self.row_gen_for_source(source_name)
        
            ri =  RowIntuiter(rg).intuit()

            source.row_spec = ri
            
            print source_name, ri
            
        self.metadata.write_to_dir()
        
    def meta_set_table(self):
        
        for source_name in self.metadata.sources:
            self.metadata.sources.get(source_name).table = 'birth_profile'
            
        self.metadata.write_to_dir()

    def meta_compile_headers(self):
        """Create column map so we can """
        import csv
        import os

        if os.path.exists(self.col_map_fn):
            headers = self.col_map
        else:
            headers = {}
        
        fn = self.filesystem.path('meta', 'headers.csv')

        with open(fn, 'w') as f:
            
            w = csv.writer(f)
            
            for source_name in self.metadata.sources:
                source = self.metadata.sources.get(source_name)
           
                rg = self.row_gen_for_source(source_name)
            
                self.log(source_name)
                w.writerow(rg.get_header())
             
                for header in rg.get_header():
                    h = header.strip().lower().replace(' ','')
                    if h not in headers:
                         headers[h] = dict(header=h, col='', description=header)
           
        with open(self.col_map_fn, 'w') as f:
            w = csv.DictWriter(f, ['header','col','description'])

            for header in sorted(headers.keys()):
                r = headers[header]
                w.writerow(r)
        
        
    def meta_add_footer_doc(self):
        """Add the file footers to the documentation"""
        
        fn = self.filesystem.path('meta', 'documentation.md')
        
        with open(fn, 'w+') as f:
            
            all_totals = {}
            
            for source_name in sorted(self.metadata.sources.keys()):
                
                source = self.metadata.sources[source_name]

                rg = self.row_gen_for_source(source_name)
                
                footer = rg.get_footer()
                
                if not footer:
                    continue
                    
                if footer[0].startswith('Total'):
                    totals = footer.pop(0)
                    totals = totals.split()
                    totals.pop(0) # Remove word 'Totals'
                    
                    header = rg.get_header()
                    header.pop(0) # Remove zip field. 
                    
                    totals = zip(header, totals)

                x = """## File Footer for Year {}
                
                {}
                
                """.format(source.time, '\n'.join(footer))
                
                self.log("Writing footer docs for {}".format(source_name))
                
                f.write(x)
            
        
    @staticmethod
    def int_null_dash(v):
        """Remove commas from numbers and cast to int"""

        if not isinstance(v, basestring):
            return int(v)
        
        v = str(v).strip()
        
        if v == '-':
            return 0

        return int(v.replace('*','').replace(',', ''))


    def build_modify_row(self, row_gen, p, source, row):
        
        row['year'] = int(source.time)
        
    def mangle_column_name(self, i, n):
        """
        Override this method to change the way that column names from the source are altered to
        become column names
        :param i: column number
        :param n: original column name
        :return:
        """
        from ambry.orm import Column
       
        col_map = { x['header']:x['col'] for  x in self.col_map.values() }
        
        munged = n.strip().lower().replace(' ','')

        return col_map[munged]


    def build_totals(self):
        """Add the file footers to the documentation"""
        
        fn = self.filesystem.path('meta', 'documentation.md')

        all_totals = {}
        
        p = self.partitions.find_or_new(table='birth_profile', grain = 'totals')
        p.clean()
        
        with p.inserter() as ins:
            for source_name in sorted(self.metadata.sources.keys()):
            
                source = self.metadata.sources[source_name]

                rg = self.row_gen_for_source(source_name)
            
                footer = rg.get_footer()
            
                if not footer:
                    continue
                
                if footer[0].startswith('Total'):
                    totals = footer.pop(0)
                    totals = totals.split()
                    totals.pop(0) # Remove word 'Totals'
                
                    header = rg.get_header()
                    header.pop(0) # Remove zip field. 
                
                    totals = dict(zip(header, totals))
                
                    totals['year'] = int(source.time)
                    
                    totals['zipcode'] = None
                    
                    self.log('Inserting totals for year {}'.format(totals['year']))
                    ins.insert(totals)
                    

    def build(self):
        
        super(Bundle,self).build()
        self.build_totals()
        return True
        
        
