

# {about_title}

{about_summary}


# County Birth Profiles

The base data is provided from CDPH aggregated to zip codes. The ``birth_profile_county`` re-aggregates to counties, using the HUD zip code to county crosswalks. These crosswalks link zip codes to the counties that contain them, and where a zip code crosses a county bounty, the crosswalk provides the portion of residential addresses that are contained in each of the counties. The births are allocated to zip codes by the monther's zip code of residence, so while the re-allocation is statistically sensible, it will most likely result in different counts than if the original data were aggregated directly to counties. 


# Caveats

The birth profiles have two special "catchall" zip codes for mssing values and low number cells:

* Zip 99998 collects the values for all cells in a column that are smaller than the lower number limit, which is documented for most years at 5. 
* Zip 99999 collects the values for all cells in a column for births where the mother's zip code is not known, or where the value is not "in the appropriate range."

Values for ``mother_age_lt29`` are recorded only for the year 1995. However, even though the <29 values would appear to overlap with the 20-29 age group, summing all of the age columns produces the same value as the ``total_births`` column. Based on this, it appears that the ``mother_age_lt29`` coulmn is just misnamed, and should be ``mother_age_lt20.``

In the catchall zip codes ( 99998 and 99999 ) for year 2006, the sum of the four birth weight columns does not equal the total_births column. 



# File Footer Documentation

These are the notes from the footers of each of the annual files. The footer text varies a bit from year to year, but is substantially similar across many years, so the following sections may have some differences with the fotters for specific years. 

## File Footer for Year 2000 to 2003
 
__ The following text appears, in a substantially similar form, after the end of data in the fiels for the years 2000, 2001, 2002 and 2003 __ 
            
* Only ZIP Codes with five events or more are listed by ZIP in this report.  All births which occurred to residents in ZIP Codes with fewer than five events
have been combined into ZIP Code "99998".

** All births to California residents with ZIP Codes missing or not in the appropriate range for California have been combined into ZIP Code "99999".

Source: California Department of Health Services, Birth Records
   
        
## File Footer for Years after 2003

__ The following text appears after the end of the data in file years after 2005, and is substantially similar to this text in years 2003 and 2004. __ 
                
- Indicates zero events. ( Translated to 0 in the Civic Knowledge conversion )
                
* Only ZIP Codes with five events or more are listed in this report.  All births which occurred to residents in ZIP Codes with fewer than five events
have been combined into ZIP Code "99998".
** All births to California residents with ZIP Codes missing or not in the appropriate range for California have been combined into ZIP Code "99999".

1.0 The "American Indian" group includes American Indian, Aleut, and Eskimo.
The "Asian" group includes Asian Indian, Chinese, Japanese, Korean, and Other Asian.
The "Southeast Asian" group includes Cambodian, Hmong, Laotian, Thai, and Vietnamese.
The "Hawaiian/Pacific Islander" group includes Hawaiian, Guamanian, Samoan, and Other Pacific Islanders.
The "Hispanic" group includes all mothers who indicated they were of Spanish/Hispanic origin, regardless of race.
The "Two + Races" group includes all non-Hispanic mothers who reported two or more of any of the race groups listed on this report.

Source: California Department of Public Health, 2006 Birth Records
                


                
                

                
                