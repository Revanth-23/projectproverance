#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

bom_messages = {
    'product_name_blank': "Please insert a product name (column A)",
    'customer_part_blank': "Please insert the  part # (column B)",
    'product_name_invalid': "A product name provided here should be present in \"tab 4 Product\", Column E (column A)",
    'customer_part_absent': "A part # provided here should be present in \"tab 2 Part sourcing\", Column A (column B)",
    'duplicate_bom_tab': "The product name, part # should not be duplicate ( column A , column B)"
}

contact_info_messages = {
    'name_blank': "Please insert the contact name (column A)",
    'email_blank': "Please insert the contact email (column B)",
    'email_invalid': "Please correct the format of the contact email (column B)",
    'duplicate_cust_contact_info_tab': "The name of contact and contact email address should not be duplicate (column A and column B)",
    'email_name_null_provided': "Please provide name of contact and contact email address if contact phone number has been provided (column A and column B)",
    'duplicate_email_diff_contact_tab': "Two different names of contacts are provided for same contact email address (column B)"
}

part_site_map_messages = {
    'partner_name_blank': "Please insert partner name (column A) || ",
    'partner_part_blank': "Please insert the partner part # (column B) || ",
    'site_number_blank': "Please insert the site number of the Tier 1 partner from \"Tab 6 Site List\"  Column A (column D) || ",
    'site_number_absent': "Please insert the site number of the Tier 1 partner from \"Tab 6 Site List\"  Column A (column D) || ",
    'activity_blank': "Please choose activity from the drop-down (column E) || ",
    'activity_picklist': "Please choose activity from the drop-down (column E) || ",
    'recovery_time_blank': "Please insert the recovery time between 1 to 52 (weeks) if the site activity has been provided (column F) || ",
    'recovery_time_invalid': "Please insert the recovery time between 1 to 52 (weeks) if the site activity has been provided (column F) || ",
    'alt_site_number_absent': "Input alternate site number from Tab '6. Site List', column A (column G) || ",
    'altsite_primarysite_same': "Please ensure alternate site is different from the primary site (column G) || ",
    'altsite_bringuptime_blank': "Please ensure alternate site bring up time is a whole number between 1 to 51 ( weeks) if alternate site number has been provided (column H) || ",
    'altsite_blank': "Please insert alternate site number if the alternate site bring up time has been provided (column G) || ",
    'altsite_bringuptime_invalid': "Please ensure alternate site bring up time  is a whole number between 1 to 51 (Weeks) and it should be less than recovery time (column H) || ",
    'bringuptime_greater_recoverytime': "Please insert the alternate site bring up time between 1 to 51 (Weeks) and it should be less than recovery time (column H) || ",
    'partner_name_absent': "Resilinc Partner Name provided here should be present in Tab '1. Partners', column A  (column A) || ",
    'partner_part_absent': "Partner Part # provided here should be present in \"Tab 2. Part Sourcing\", Column F (column B) || ",
    'partner_part_same': "Please ensure the partner part # corresponds with its respective partner name (column B) || ",
    'partner_not_same_for_site': "Please ensure the site number corresponds with its respective partner name (column D) || ",
    'partner_not_same_for_alt_site': "Please ensure the partner name of the alternate site matches the partner name of the primary site (column D and column G ) || ",
    'duplicate_pname_ppart_snum': "The Site number, resilinc partner name, partner part, activity, recovery time, alternate site number, alternate site bring up time should not be duplicate (column D, column I, column B, column E,column G and column H) || "
}

sitemapping_messages = {
    'to_site_number_blank': "Provide \"To Site Number\" from \"Tab 6. Site List\", Column A (column B)",
    'to_site_number_absent': "Provide \"To Site Number\" from \"Tab 6. Site List\", Column A (column B)",
    'from_site_number_absent': "Provide \"From Site Number\" from \"Tab 6. Site List\", Column A (column A)",
    'from_site_number_blank': "Provide \"From Site Number\" from \"Tab 6. Site List\", Column A (column A)",
    'identical_fromsite_tosite': "\"From Site Number\" and \"To Site Number\" should be different (column A-B) ",
    'partner_part_absent': "\"Partner Part #\" provided here should be present in \"Tab 2. Part Sourcing\", Column F (column C)",
    'partner_part_invalid': "\"Partner Part #\" and \"From Site Number\" should correspond to the same partner",
    'duplicate_fromsite_tosite': "\"From Site Number\" and \"To Site Number\" should not be duplicate (column A-B)"
}
