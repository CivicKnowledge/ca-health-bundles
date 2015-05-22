""""""

from ambry.bundle.loader import LoaderBundle
from ambry.bundle.rowgen import GeneratorRowGenerator


class Bundle(LoaderBundle):

    """"""

    def meta_schema(self):
        """Create the table from the row generator"""
        self.meta_intuit_table('facilities_index', self.row_generator)
        self.schema.write_schema()
        
    @property
    def row_generator(self):
        """A row generator function that joins two library tables"""
        c = self.library.dep('cross').partition
        f = self.library.dep('facilities').partition
        g = self.library.dep('geoids').partition
        
        c.attach(f,'f')
        c.attach(g,'g')
        
        """
        """
        
        # There are multiple entries for each facility, so take the most recent
        q = """SELECT DISTINCT cross.oshpd_id, cross.oshpd_license_number, cross.oshpd_facility_status, cross.cdph_od_facid, 
        cross.l_c_provider_number, cross.l_c_facility_id, cross.oshpd_facility_number, cross.oshpd_perm_id, 
        cross.oshpd_facility_level, cross.parent_oshpd_id, cross.l_c_license_number,
        fac.total_number_beds, fac.facility_status_date, fac.dba_city, fac.facility_name, fac.county_name, 
        fac.license_type_desc, fac.license_category_desc, fac.dba_zip_code, fac.er_service_level_desc, 
        fac.facility_status_desc, fac.county_code, fac.type, fac.dba_address1,
        geo.*
        FROM facility_cross AS cross
        LEFT JOIN f.facilities AS fac ON cross.oshpd_id = fac.oshpd_id
        LEFT JOIN g.facilities_geoids  AS geo ON fac.id = geo.facilities_id 
        WHERE cross.oshpd_id IS NOT NULL GROUP BY cross.oshpd_id ORDER BY year desc """
        
        row_proxy = c.query(q)
        
        def rrg():

            for i, row in enumerate(row_proxy):
                
                if i == 0:
                    yield row.keys()
                
                yield row
   
        return GeneratorRowGenerator(rrg)
   
   
    def build_modify_row(self, row_gen, p, source, row):

        row['type_code'] = row['oshpd_id'][:3]

        if 'facilities_id' in row: # Just the last part of the OSHPD id, i think. 
            del row['facilities_id']

        del row['id']
        
    def build(self):
        from ambry.util.datestimes import expand_to_years
        
        # First build the full table, with all of the OSHPD ids, names, etc. 
        
        self.log("Build full facilities data set")
        facilities = self.partitions.find_or_new(table='facilities')
        facilities.clean()
        
        self.build_from_row_gen(self.row_generator, facilities)

        facilities.close()

        # Then build the index, which has just the OSHPD ids. 
        self.log("Build the index without years")
        p = self.partitions.find_or_new(table='facilities_index')
        p.clean()
        
        with p.inserter() as ins:
            for  row in facilities.rows:
                ins.insert(row)
            
        p.close()

        # Now build the index with years
        self.log("Build the index with years")
        p = self.partitions.find_or_new(table='facilities_index', time=self.metadata.about.time)
        p.clean()

        with p.inserter() as ins:
            for row in facilities.rows:
                row = dict(row)
                del row['id']
                for year in expand_to_years(self.metadata.about.time):   
                    row['year'] = year
                    ins.insert(row)


        p.close()
    

        return True
        
  

        
        
