
# {about_title}

{about_summary}



I used the following sources of data and included all records whether matched or unmatched.  The Licensed Facility Listing Report for all types of facilities (http://hfcis.cdph.ca.gov/Reports/GenerateReport.aspx?rpt=FacilityListing), the CDPH Facility list that is on the Open Data Portal  (https://cdph.data.ca.gov/Facilities-and-Services/Healthcare-Facility-Locations/n8ju-ifrh) and OSHPDs Licensed Facility Dimension table from our data warehouse (which is more comprehensive than the facility lists on OSHPDs website -  http://www.oshpd.ca.gov/hid/Products/Listings.html) and merged them together based on license number as a starting point.
 
I also added L&C Facility ID, Provider Number and address information, all of OSHPDs identifying numbers, and OSHPDs facility level which helps to identify the differences in the way L&C records facilities vs. the way OSHPD does. I also included address and other geography.  The dataset is sorted by OSHPD ID since it is an OSHPD product. Due to unmatched CDPH records OSHPDs do not show in the list until row 3200.
As an example on row 3200 you'll see OSHPD Parent ID 106010735 - Alameda Hospital.  There are 12 record combinations for this hospital, but only 3 physical locations, each having the same OSHPD Parent ID and License number.  OSHPD recognizes 3 facility locations, however, CDPH recognizes the SNF beds at the Clinton address as a separate "facility,"  thus there is an extra record. OSHPD Facility Level will help identify distinct facilities. 
 
Just by matching on address across the lists you can see why it is so difficult to match across departments or even across programs.  Differences in address, name, type of facility, identifying numbers, etc. all lend to the multiple matches you will see in the attached spreadsheet.  It's a daunting task you are attempting and I wish you well. 