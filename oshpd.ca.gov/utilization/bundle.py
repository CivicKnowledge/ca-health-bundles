'''

'''

from  ambry.bundle.loader import ExcelBuildBundle
 
class Bundle(ExcelBuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def build(self):

        return True
        
    def gen_rows(self, source=None, as_dict=False, segment = None):
        """Adapt gen_rows to skip two of the header lines, and save the numbers in the fourth.  """
        from collections import OrderedDict
        
        prefix_headers = ['id', 'year','noresp']
        
        g = super(Bundle, self).gen_rows(source=source, as_dict = False,
                                          segment = segment, prefix_headers = prefix_headers)
        
        _ = g.next()
        _ = g.next()
        
        header,numbers = g.next()
        
        header = OrderedDict(zip(header, [ "{}: {} ".format(n,h) for h,n in zip(header,numbers)]))
      
        for _, row in g:
         
            yield header, row
        
    def meta(self):
        from collections import OrderedDict
        
        self.database.create()

        if not self.run_args.get('clean', None):
            self._prepare_load_schema()

        with self.session:
            for source_name, source in self.metadata.sources.items():

                for segment, table_name in ( (1,"sections14"), (2,"sections57"), (3,"sections14"), (4,"sections57") ):

                    table = self.schema.add_table(table_name)

                    header, row = self.gen_rows(source_name, segment=segment, as_dict=False).next()

                    self.schema.update_from_iterator(table.name,
                                       header = self.mangle_header(header),
                                       iterator=self.gen_rows(source_name, segment=segment, as_dict=False),
                                       max_n=1000,
                                       logger=self.init_log_rate(500))

        return True
        
    def build(self):

        for p in self.partitions:
            p.clean()

        for source_name, source in self.metadata.sources.items():

            for segment, table_name in ( (1,"sections14"), (2,"sections57"), (3,"sections14"), (4,"sections57") ):

                p = self.partitions.find_or_new(table=table_name)

                self.log("Loading source '{}' into partition '{}'".format(source, p.identity.name))

                lr = self.init_log_rate(print_rate = 5)

                header = [c.name for c in p.table.columns]

                with p.inserter() as ins:
                   for _, row in self.gen_rows(source_name, segment = segment):
                       lr(str(p.identity.name))

                       d = dict(zip(header, row))

                       ins.insert(d)
        

        
        
        

