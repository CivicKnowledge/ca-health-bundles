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
        self.build_aggregate_counties()
        return True
        
        
    def build_aggregate_counties(self):
        
        import pandas as pd
        import numpy as np
        from geoid import civick, census

        df = self.partitions.all[0].pandas
        zc = self.library.dep('zip_county').partition.pandas
        
        zcm = pd.merge(df,zc, left_on='zipcode',right_on='zip')

        all_prenat_cols = [ u'prenatal_first', u'prenatal_none', u'prenatal_second', u'prenatal_third', u'prenatal_unk' ]

        all_age_cols = [u'mother_age_lt20', u'mother_age_lt29', u'mother_age_20_29', u'mother_age_30_34', 
         u'mother_age_gt35', u'mother_age_unk']

        all_weight_cols = [  u'weight_1500_2499', u'weight_gt2500', u'weight_lt1500', u'weight_unk']

        all_race_cols = [ u'mother_race_white', u'mother_race_black',
         u'mother_race_asian', u'mother_race_asianpi', u'mother_race_seasian', u'mother_race_hisp',
         u'mother_race_amind', u'mother_race_filipino', u'mother_race_hpi', u'mother_race_multiple', 
         u'mother_race_other']

        for v in (all_age_cols+all_weight_cols+all_race_cols+all_prenat_cols+[u'total_births']):
            zcm[v] *= zcm.res_ratio

        zcm = zcm.groupby(['county','year']).sum()
        zcm = zcm.reset_index()
        
        for v in (all_age_cols+all_weight_cols+all_race_cols+all_prenat_cols+[u'total_births']):
            zcm[v] = np.round(zcm[v],0)
        
        zcm['total_births_prenat'] = zcm[all_prenat_cols].sum(axis=1).astype('int')
        zcm['total_births_age'] = zcm[all_age_cols].sum(axis=1).astype('int')
        zcm['total_births_weight'] = zcm[all_weight_cols].sum(axis=1).astype('int')
        zcm['total_births'] = zcm['total_births'].astype('int')
        
        # Need to get rid of columns not in table, or intserter will throuw a cast error. 
        final_cols = [u'county', u'year', 
          u'mother_age_lt20', u'mother_age_20_29', u'mother_age_lt29', u'mother_age_30_34', u'mother_age_gt35', u'mother_age_unk',
          u'mother_race_white', u'mother_race_black', u'mother_race_asian', u'mother_race_asianpi', 
          u'mother_race_seasian', u'mother_race_hisp', u'mother_race_amind', u'mother_race_filipino', 
          u'mother_race_hpi', u'mother_race_multiple', u'mother_race_other', 
          u'prenatal_first', u'prenatal_none', u'prenatal_second', u'prenatal_third', u'prenatal_unk', 
          u'total_births', u'weight_1500_2499', u'weight_gt2500', u'weight_lt1500', u'weight_unk']
        
        zcm = zcm[final_cols]
        
        p = self.partitions.find_or_new(table='birth_profile_county')
        lr = self.init_log_rate()
        
        with p.inserter() as ins:
            for row in zcm.iterrows():
                row =  row[1].to_dict()
                
                row['county_gvid'] = str(census.County.parse('{:>05d}'.format(int(row['county']))).convert(civick.County))
                del row['county']
                lr(str(p.identity.name))
                
                ins.insert(row)
                
        
    def test(self):
        
        import numpy as np
        
        df = self.partitions.all[0].pandas

        all_prenat_cols = [ u'prenatal_first', u'prenatal_none', u'prenatal_second', u'prenatal_third', u'prenatal_unk' ]

        all_age_cols = [u'mother_age_lt20', u'mother_age_lt29', u'mother_age_20_29', u'mother_age_30_34', 
         u'mother_age_gt35', u'mother_age_unk']

        all_weight_cols = [  u'weight_1500_2499', u'weight_gt2500', u'weight_lt1500', u'weight_unk']

        all_race_cols = [ u'mother_race_white', u'mother_race_black',
        u'mother_race_asian', u'mother_race_asianpi', u'mother_race_seasian', u'mother_race_hisp',
        u'mother_race_amind', u'mother_race_filipino', u'mother_race_hpi', u'mother_race_multiple', 
        u'mother_race_other']
        
        df['total_births_prenat'] = df[all_prenat_cols].sum(axis=1).astype('int')
        df['total_births_age'] = df[all_age_cols].sum(axis=1).astype('int')
        df['total_births_weight'] = df[all_weight_cols].sum(axis=1).astype('int')
        df['total_births'] = df['total_births'].astype('int')

        assert (all(df['total_births_prenat'] == df['total_births' ]))
        assert (all(df['total_births_age'] == df['total_births' ]))
        assert (all(df['total_births_age'] == df['total_births' ]))
        
        # The 99998 and 99999 catch-all zipcodes are broken for 2006
        not_broke = np.logical_not(((df.zipcode == 99998) | (df.zipcode == 99999)) &  (df.year == 2006))
        

        assert (all(df[not_broke]['total_births_weight'] == df[not_broke]['total_births' ]))
        
        
         
        
        
        
        
