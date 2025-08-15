No, I haven’t checked the code into Git yet, nor have I added it to the ADTF section of the wiki. I’ll coordinate with Jim to complete both of these tasks.

As you know, I’m currently fully focused on developing the DM table creation scripts for the ETLs. I kindly request a few more days to complete my assigned work. Once that is done, I’ll proceed with checking in the code and updating the wiki page with more meaningful documentation.

Regarding the current implementation:

The logic prioritizes population data from the source table for insert records, and from the target table for updates.

If no data is found in the target based on the given criteria for an update, it will attempt to insert from the source and then perform an update.

If neither source nor target have the required data, it will generate synthetic data (for both insert and update scenarios).

While inserting from the source, if the target table has additional columns, the implementation will generate synthetic values for those columns.

It also ensures all integrity constraints are respected and handles data for virtual columns appropriately.