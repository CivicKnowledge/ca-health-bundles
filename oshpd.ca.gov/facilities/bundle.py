from  ambry.bundle.loader import ExcelBuildBundle

class Bundle(ExcelBuildBundle):
    ''' '''
    
    def build(self):
        
        #self.build_load()
        super(Bundle, self).build()
        self.build_addresses()
        self.build_block_cross()
        return True
    
    
    def build_modify_row(self, row_gen, p, source, row):
        """Called for every row to allow subclasses to modify rows. """
        from xlrd import xldate_as_tuple
        from datetime import date

        row['year'] = int(source.time)
        row['facility_status_date']  =  date(*xldate_as_tuple(row['facility_status_date'],row_gen.workbook.datemode)[:3])
        row.update(source.row_data.dict)
        
    def build_addresses(self):
        """Geocode the addresses and build an address table"""
        
        from ambry.geo.geocoders import DstkGeocoder

        facilities = self.partitions.find(table='facilities')

        def address_gen():
            for row in facilities.query("SELECT * FROM facilities"):
                address = "{}, {}, {} {}".format(row['dba_address1'], row['dba_city'], 'CA', row['dba_zip_code'])
                yield (address, row)

        dstk_service = self.config.service('dstk')
        
        dstk_gc = DstkGeocoder(dstk_service, address_gen())
        
        p = self.partitions.find_or_new(table='facilities_addresses')
        p.clean()
        
        lr = self.init_log_rate(500)
        
        with p.inserter() as ins:
            for i, (k, r, inp_row) in enumerate(dstk_gc.geocode()):
                lr("Addresses "+str(i))
                r['facilities_id'] = inp_row['id']
                ins.insert(r)
            
    def build_block_cross(self):
        """Build the facilities_blockgroups crosswalk file to assign facilities to blockgroups. """
        from ambry.geo.util import find_geo_containment, find_containment
        from geoid import civick 

        lr = self.init_log_rate(3000)

        def gen_bound():
            
            boundaries = self.library.dep('blockgroups').partition

            # Note, ogc_fid is the primary key. The id column is created by the shapefile. 
            for i,boundary in enumerate(boundaries.query(
                "SELECT  AsText(geometry) AS wkt, gvid FROM blockgroups")):
                lr('Load rtree')
     
                yield i, boundary['wkt'] , boundary['gvid'] 
        
        def gen_points():

            for row in self.partitions.find(table = 'facilities_addresses').rows:
                if  row['longitude'] and row['latitude']:
                    yield (row['longitude'], row['latitude']), row['facilities_id']


        p = self.partitions.find_or_new(table='facilities_geoids')
        p.clean()

        with p.inserter() as ins:
            for point, point_o, cntr_geo, cntr_o in find_containment(gen_bound(),gen_points()):

                blockgroup_gvid = civick.Blockgroup.parse(cntr_o)
                tract_gvid = blockgroup_gvid.convert(civick.Tract)
                county_gvid = blockgroup_gvid.convert(civick.County)
                
                ins.insert(dict(facilities_id = point_o, 
                                blockgroup_gvid = str(blockgroup_gvid),
                                tract_gvid = str(tract_gvid),
                                county_gvid = str(county_gvid)
                                ))
                                
                lr('Marking point containment')
                
    def build_index(self):
        """Reduce the other tables into an index file. """
        
        
        geoids = self.partitions.find_or_new(table='facilities_geoids')
        addresses = self.partitions.find_or_new(table='facilities_addresses')
        facilities = self.partitions.find(table='facilities')
        
        facilities.attach(addresses,'addresses')
        facilities.attach(geoids,'geoids')
        
        q = """
        SELECT year, type, oshpd_id, facility_name, dba_city, dba_zip_code, blockgroup_gvid, tract_gvid,  county_gvid
        FROM facilities
        JOIN geoids.facilities_geoids AS geoids ON geoids.facilities_id = facilities.id
        JOIN addresses.facilities_addresses AS addresses ON addresses.facilities_id = facilities.id
        """
        
        p = self.partitions.find_or_new(table='facilities_index')
        p.clean()
        lr = self.init_log_rate()
        
        with p.inserter() as ins:
            for row in facilities.query(q):
                ins.insert(row)
                lr(str(p.identity))
                
    def redo_finalize(self):
        
        for p in self.partitions.all:
            p.finalize(force=True)
        
        
        