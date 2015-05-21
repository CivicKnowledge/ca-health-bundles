
# {{about.title}}

{{about.summary}}

## Notes

Although the source of this data is OSHPD, the names of the hospitals don't link exactly to the OSHPD facilities list. Instead, they are connected to the CDPH facilities list, from bundle cdph.ca.gov-facilities.

However, they match well enough to the OSHPD list that nearly all of the entries can be assigned an OSHPD facility id. When the ``oshpd_id`` column is set from an inexact hospital name match, the name of the matched facility is stored in the ``matched_hospital_name`` column. If the ``oshpd_id`` field is empty, the ``matched_hospital_name`` should also be empty, indicating that the entry was not matched to a facility in the OSHSPD facility index. If the ``matched_hospital_name`` field is empty, but ``oshpd_id`` is set, it means that the hospital was matched on an exact name. 

