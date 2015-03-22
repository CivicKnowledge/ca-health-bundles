'''

'''

from  ambry.bundle import BuildBundle
 


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def generate_rows(self, source):
        
        col_labels = self.filesystem.read_yaml('meta','variable_labels.yaml')

    def meta_labels(self):
        """Read the labels for the variables and code values for the variables, using the 
        Stata reader. """
        import re
        import os
        import struct
        import pandas as pd

        from pandas.io.stata import StataReader
   
        var_labels = None
        val_labels = None

        if not os.path.exists(self.filesystem.path('meta','variable_labels.yaml')):

            for name, fn in self.sources():
   
                if name.endswith('l'):

                    self.log("Getting labels for {}  from {} (This is really slow)".format(name, fn))
   
                    reader = StataReader(fn)

                    df = reader.data() # Can't get labels before reading data
            
                    var_labels = reader.variable_labels()
                    val_labels = reader.value_labels()
                    
                    break
                    
                    
            self.filesystem.write_yaml(var_labels, 'meta','variable_labels.yaml')
            self.filesystem.write_yaml(val_labels, 'meta','value_labels.yaml')
            
        else:
            self.log("Skipping extracts; already exist")

        # The value codes include both the value codes and the imputation codes. The imputation codes
        # are extracted  as positive integers, when they really should be negative. 
        table_values = {}
        imputation_values = {}
        
        if not val_labels:
            val_labels = self.filesystem.read_yaml('meta','value_labels.yaml')
            
        for k,v in val_labels.items():
            table_values[k] = {}
            imputation_values[k] = { -10:  'NO IMPUTATION' }
        
            for code, code_val in v.items():
                
                signed_code = struct.unpack('i',struct.pack('I',int(code)))[0] # Convert the unsigned to signed
                
                if signed_code < 0:
                    imputation_values[k][signed_code] = code_val
                else:
                    table_values[k][code] = code_val

        self.filesystem.write_yaml(table_values, 'meta','table_codes.yaml')
        self.filesystem.write_yaml(imputation_values, 'meta','imputation_codes.yaml')
            
        self.log("{} table variables".format(len(table_values)))
        self.log("{} imputation variables".format(len(imputation_values)))

        return True
        

    def build_naive_import(self):
        """
        Load the SAS files in to a database with Pandas. This is an odd thing to do in the 
        meta phase, but it makes everything thing else easier later. 
        
        To load the imputation flags, we have to use the SAS file, since the Stata file can't
        be loaded by the code in Pandas"""
        import re
        import pandas as pd
        import os
    
        import sqlalchemy
        
        from sas7bdat import SAS7BDAT

        for name, fn in self.sources():
            if name.endswith('l'):
                continue

            fn = self.source(name)
            
            self.log("Loading {}".format(fn))

            sf = SAS7BDAT(fn)
    
            import_db = self.filesystem.build_path(name+'.db')
    
            if os.path.exists(import_db):
                self.log("Skipping {}; already exists".format(import_db))
                continue
    
            self.log("Importing {} ".format(name))
            rows = [ row for row in sf.readData()]

            df = pd.DataFrame(rows[1:])
            df.columns = rows[0]
            print df.head()
 
            engine = 'sqlite:///'+import_db
 
            df.to_sql(name,sqlalchemy.create_engine(engine))

    def meta_schema(self):
        import sqlalchemy
        
        val_labels = self.filesystem.read_yaml('meta','variable_labels.yaml')
        
        t = self.schema.add_table('chis_puf', description='CHIS public use data')
        self.schema.add_column(t,'id',datatype = 'integer',is_primary_key = True)
        
        for i , (k, desc) in enumerate(sorted(val_labels.items(), key=lambda x: x[0])):
            self.log("Adding column {}, {}".format(i,k))
            self.schema.add_column(t,k, datatype = 'integer', description = desc)
       
        tif = self.schema.add_table('chis_puf_x', description = 'CHIS public use imputation flags')
        self.schema.add_column(tif,'id',datatype = 'integer',is_primary_key = True)
        
        for i , (k, desc) in enumerate(sorted(val_labels.items(), key=lambda x: x[0])):
            self.log("Adding column {}, {}".format(i,k))
            self.schema.add_column(tif,k+'_x', datatype = 'integer', description = 'Imputation flag for: '+desc)


        self.schema.write_schema()
        

    def meta_find_int(self):
        """Look for columns that have non-int values and convert the schema entry to real. """
        import sqlite3
        from ambry.orm import Column
        
        
        conn = sqlite3.connect(self.filesystem.build_path('chis12d.db'))
        conn.row_factory = sqlite3.Row

        notint = set()

        lr = self.init_log_rate(500)
        for row in conn.execute('SELECT * FROm chis12d limit 10000'):
            row = dict(zip(row.keys(),row))
    
            for k,v in row.items():
                if int(v) != v:
                    notint.add(Column.mangle_name(k))
            lr()
            
        with self.session:
            t = self.schema.table('chis_puf')
            for c in t.columns:
                
                if c.name in notint:
                    self.log(c.name)
                    c.datatype = c.DATATYPE_REAL
                else:
                    c.datatype = c.DATATYPE_INTEGER
            
        self.schema.write_schema()
       
    def meta_codes(self):
        
        table_codes = self.filesystem.read_yaml( 'meta','table_codes.yaml')
        self.log('{} table codes'.format(len(table_codes.keys())))
        
        t = self.schema.table('chis_puf')
        
        def get_codes(col, codes):
            """The code yaml files have wacky values"""
            
            if col.endswith('_p'):
                col = col.replace('_p','')
            
            return codes.get(col, codes.get(col+'x', {}))

        with self.session:
            for c in t.columns:
                for k,v in get_codes(c.name, table_codes).items():
                    c.add_code(k,v)
        
        impt_codes = self.filesystem.read_yaml('meta','imputation_codes.yaml')
        self.log('{} impt codes'.format(len(impt_codes.keys())))
        
        t = self.schema.table('chis_puf_x')
        with self.session:
            for c in t.columns:
                for k,v in get_codes(c.name, impt_codes).items():
                    cd = c.add_code(k,v)
        
        self.schema.write_codes()
                
    def meta(self):
        self.meta_labels()
        self.meta_schema()
        self.build_naive_import()
        self.meta_find_int()
        self.meta_write_codes()

    def build(self):
        import sqlite3
        
        self.build_naive_import()
        
        ## Main data file
        conn = sqlite3.connect(self.filesystem.build_path('chis12d.db'))
        conn.row_factory = sqlite3.Row
        
        p = self.partitions.find_or_new(table = 'chis_puf')
        lr = self.init_log_rate(5000)
        
        if not p.is_finalized:
            with p.inserter() as ins:
                for row in conn.execute('SELECT * FROm chis12d'):
            
                    lr(str(p.identity))
                    ins.insert(dict(zip(row.keys(),row)))
        
            conn.close()
            p.finalize()
        
        ## Imputation flags
        conn = sqlite3.connect(self.filesystem.build_path('chis12f.db'))
        conn.row_factory = sqlite3.Row
        
        p = self.partitions.find_or_new(table = 'chis_puf_x')
        lr = self.init_log_rate(5000)
        
        if  not p.is_finalized:
            with p.inserter() as ins:
                for row in conn.execute('SELECT * FROM chis12f'):
            
                    lr(str(p.identity))
                    
                    ins.insert(dict(zip(row.keys(),row)))
               
            conn.close()
            p.finalize()
            
        return True
        

        
    def test_codes(self):
        
        t = self.schema.table('chis_puf')
        
        import pprint, json
        
        print json.dumps(t.nonull_col_dict, indent=4)
        
        
        
        
