""""""

from ambry.bundle import BuildBundle


class Bundle(BuildBundle):

    """"""

    def __init__(self, directory=None):

        super(Bundle, self).__init__(directory)

    def meta_build_schema(self):
        
        p = self.library.dep('pddpuf').partition
        
        ac = self.schema.add_column
        
        with self.session:
            for v in self.metadata.build.summary_cols:
                c = p.table.column(v)
                
                t = self.schema.add_table('pdd_summary_'+str(v), 
                    description = "Patient Discharge counts, aggregated on the {} variable ".format(v))
                t.add_id_column()
                t.add_column('oshpd_id', datatype = 'varchar', description=p.table.column('oshpd_id').description,
                             proto_vid = 'oshpd_facility.oshpd_id')
                t.add_column('year', datatype = 'integer', proto_vid = 'oshpd_facility.year',
                             description=p.table.column('dschyear').description )
                t.add_column(v, datatype=c.datatype, proto_vid=c.id_, description = c.description, data={'aggvar':1})
                t.add_column('count', datatype = 'integer', derivedfrom=c.id_, description = "Count of records")
    
        self.schema.write_schema()
        
    def build(self):

        for v in self.metadata.build.summary_cols:
            self.build_aggregate(v)
          

        return True
        
    def build_aggregate(self, v):
        
        self.log("Build aggregate for: {}".format(v))
        
        p = self.library.dep('pddpuf').partition

        q = "SELECT id, oshpd_id, dschyear as year,  {} FROM pdd_puf_c".format(v)

        df = p.select(q,index_col=p.get_table().primary_key.name).pandas
 
        dfg =  df.groupby(['year','oshpd_id',v]).count().reset_index()
 
        dfg.columns = ["year", "oshpd_id", v, "count" ]
 
        out_p = self.partitions.new_partition(table = 'pdd_summary_{}'.format(v))
        out_p.clean()
        
        lr = self.init_log_rate(10000)
        
        with out_p.inserter() as ins:
            for r in dfg.sort(['oshpd_id','count'], ascending = False).itertuples(index=False):
                d =  dict(zip(dfg.columns, r))
                
                ins.insert(d)
                lr()
                
    def build_pivot_aggregate(self):
        
        
        df = self.partitions.find(table ='pdd_summary_mdc').pandas
        
        print df.describe()
    
        
        df = b.partitions.find(table ='pdd_summary_mdc').pandas
        dfmi = df.set_index(['oshpd_id','year','mdc'])
        dfmi = dfmi.drop('id',1).unstack().fillna(0)
        dfmi.to_csv('count_by_mdc.csv')
        
        
    def mk_mdc_msdrg_table(self, mdc, columns):
        
        table_name = 'pdd_mdc_'+mdc
     
        if  self.schema.table(table_name):
            return table_name
        
        msdrg_cols = columns[2:]
        
        with self.session:
            
            t = self.schema.add_table(table_name, 
                description = "MSDRGs counts per hospital, per year, for MDC {}".format(mdc))
            t.add_id_column()
            t.add_column('oshpd_id', datatype = 'varchar', proto_vid = 'oshpd_facility.oshpd_id', 
                        description='OSHPD facility id' )
            t.add_column('year', datatype = 'integer', proto_vid = 'oshpd_facility.year',
                         description='Year of discharge' )
                         
            for col in columns[2:]:
                t.add_column('msdrg_'+col, datatype='integer', description = "Counts for MS-DRG {}".format(col))
                
            self.schema.write_schema()
            
            return table_name
        
        
    def build_mdc_msdrg(self):
        
        in_p = self.library.dep('pddpuf').partition
        
        for i in range(1,25):
            
            mdc = '{:02d}'.format(i)
            
            self.log("Processing MDC "+mdc)

            q = "SELECT id, oshpd_id, dschyear as year,  msdrg FROM pdd_puf_c WHERE mdc = '{}' limit 20000".format(mdc)

            df = in_p.select(q).pandas
           
            dfx = df.groupby(['oshpd_id','year','msdrg']).count().unstack().fillna(0)
            
            dfr = dfx.reset_index()
            dfr.columns =  dfr.columns.droplevel()
            dfr.columns = ['oshpd_id','year'] + [ 'msdrg_'+i for i in list(dfr.columns)[2:] ]
        
            table_name= self.mk_mdc_msdrg_table(mdc,dfr.columns)
            
            p = self.partitions.find_or_new(table = table_name)
            p.clean()
            
            lr = self.init_log_rate(10000)
            
            with p.inserter() as ins:
                for r in dfr.sort('oshpd_id').itertuples(index=False):
                    d =  dict(zip(dfr.columns, r))

                    ins.insert(d)
                    lr(table_name)
        
            
            
            
            
            
            
            
        
        
        
            
