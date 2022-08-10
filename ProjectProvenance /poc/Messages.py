class Messages(object):

    partners_messages = {
        'partner_name_blank'                    : "Please provide partner name (column A) || ",
        'partner_type_picklist'                 : "Please choose partner type from the drop-down (column C) || ",
        'partner_type_unique'                   : "For a duplicate partner entry, the partner type should be unique for each entry (column AI-C) || ",
        'partner_contact_name_blank'            : "Please provide partner contact name as email has been provided ( Column G) || ",
        'partner_contact_email_blank'           : "Please provide partner contact email as partner contact name has been provided (Column H) || ",
        'partner_contact_name_email_blank'      : "Please provide partner contact name and partner contact email as partner contact work phone has been provided (Column G and H) || ",
        'contact_email_invalid'                 : "Please check and correct the format of the partner contact email (column H) || ",
        #'scv_required_picklist'                 : "J: Supply Chain Mapping Required must be from PickList. ",
        #'bcp_required_picklist'                 : "K: BCP Required must be from PickList. ",
        'financial_risk1_invalid'               : "Please enter a value between 1 to 10 for financial risk 1 credit score (column Q) || ",
        'financial_risk2_invalid'               : "Please enter a value between 1 to 10 for financial risk 2 health score (column R) || ",
        'financial_risk3_invalid'               : "Please enter a value between 1 to 10 for financial risk 3 debt rating score (column S) || ",
        'financial_risk4_invalid'               : "Please enter a value between 1 to 10 for financial risk 4 Z score (column T) || ",
        'custom_defined_risk1_invalid'          : "Please enter a value between 1 to 10 for partner custom defined risk score 1 (column U) || ",
        'custom_defined_risk2_invalid'          : "Please enter a value between 1 to 10 for partner custom defined risk score 2 (column V) || ",
        'custom_defined_risk3_invalid'          : "Please enter a value between 1 to 10 for partner custom defined risk score 3 (column W) || ",
        'custom_defined_risk4_invalid'          : "Please enter a value between 1 to 10 for partner custom defined risk score 4 (column X) || ",
        'custom_defined_risk5_invalid'          : "Please enter a value between 1 to 10 for partner custom defined risk score 5 (column Y) || ",
        'emergency_contact_name_blank'          : "Please provide partner emergency contact name as email has been provided (Column AC) || ",
        'emergency_contact_email_blank'         : "Please provide partner emergency contact email as name has been provided (Column AD) || ",
        'emergency_contact_name_email_blank'    : "Please provide partner emergency contact name and partner emergency contact email as partner emergency contact work phone has been provided (Column AC and AD) || ",
        'emergency_contact_email_invalid'       : "Please correct the format of the partner emergency contact email (column AD) || ",
        'tier_level_invalid'                    : "Tier level should be in whole number (Column AG) || ",
        'higher_tier_blank'                     : "For partners with tier level 2 and above, higher tier level partner is mandatory (column AH) || ",
        'higher_tier_empty'                     : "For partners with tier level 1, higher tier level partner should be blank (column AH) || ",
        'higher_tier_partner_invalid_tier'      : "Higher tier partner should be present at tier level {0} in Column A. Please provide complete hierarchy || ",
        'invalid_duns'                          : "Please provide DUNS number up to 9 digit (column AF) || ",
        'skip_categories'                       : "As of now CDIF load only supports Tier 1 level Supplier's categories (column AG and E) || ",
        'partners_duplicate'                    : "The resilinc partner name, partner contact email, partner emergency contact email, tier level and resilinc higher tier partner should not be duplicate (column AI, column H, column AD, column AG and column AJ) (If different risk scores are provided, then maximum value of risk score will be uploaded) || "
    };

    partsourcing_messages = {
        'customer_part_blank'                       : "Please insert the Part # (column A) || ",
        'split_invalid'                             : "Please insert a whole number (between 0 to 100) for the split % (column N) || ",
        'split_sum_invalid'                         : "Please change the split % to make the sum 100 (column N) || ",
        'partner_name_blank'                        : "Please insert the partner name (column E) || ",
        'partner_name_invalid'                      : "Partner name provided here should always be a tier 1 partner in tab 1, Column A (column E) || ",
        'partner_part_blank'                        : "Please insert the Partner Part # (column F) || ",
        'qualification_status_invalid'              : "Please choose the qualification status amongst 'Qualified' or 'In Progress' or 'Going EoL' (column G) || ",
        'annual_spend_invalid'                      : "Annual spend should be up to 12 digit number (column W) || ",
        'sourcing_field_invalid'                    : "Please choose the sourcing field amongst 'Sole-Sourced' or 'Single-Sourced' or 'Multi-Sourced' (column M) || ",
        'custom_defined_risk1_invalid'              : "Please enter a value between 1 to 10 for part custom defined risk score 1 (column O) || ",
        'custom_defined_risk2_invalid'              : "Please enter a value between 1 to 10 for part custom defined risk score 2 (column P) || ",
        'custom_defined_risk3_invalid'              : "Please enter a value between 1 to 10 for part custom defined risk score 3 (column Q) || ",
        'custom_defined_risk4_invalid'              : "Please enter a value between 1 to 10 for part custom defined risk score 4 (column R) || ",
        'custom_defined_risk5_invalid'              : "Please enter a value between 1 to 10 for part custom defined risk score 5 (column S) || ",
        'part_category_unique'                      : "One part # should have unique part category (column A-D) || ",
        'custpart_partnername_partnerpart_invalid'  : "The part #, resilinc partner name, and partner part # should not be duplicate (column A , column X and column F) (If different risk scores are provided, then maximum value of risk score will be uploaded) || ",
        'multiple_spends'                           : "Annual Spend provided in the first row will be loaded if the same partner part # has different annual spends (column F-W) || ",
        'multiple_description'                      : "Description provided in the first row will be loaded if same Part # has different descriptions. (column B) || ",
        'either_cpn_ppn'                            : "Please insert at least Part # or combination of Partner Part # and Partner name (column A, column F, column E) || ",
        'scv_false'                                 : "Supply chain mapping required flag has been set to false as Part # is not provided and only Partner Part # and Partner name are provided (column T, column A, column F, column E) || "
    };

    bom_messages = {
        'product_name_blank'      : "Please insert a product name (column A) || ",      
        'customer_part_blank'     : "Please insert the  part # (column B) || ",
        'product_name_invalid'    : "A product name provided here should be present in \"tab 4 Product\", Column E (column A) || ",
        'customer_part_absent'    : "A part # provided here should be present in \"tab 2 Part sourcing\", Column A (column B) || ",
        'duplicate_bom_tab'       : "The product name, part # should not be duplicate ( column A , column B) || "
    };
        
    product_messages = {
        'product_name_blank'    : "Please insert the product name (column G) || ",
        'revenue_q1_invalid'    : "Revenue should be up to 12 digit number Revenue Q1 (column I) || ",
        'revenue_q2_invalid'    : "Revenue should be up to 12 digit number Revenue Q2 (column J) || ",
        'revenue_q3_invalid'    : "Revenue should be up to 12 digit number Revenue Q3 (column K) || ",
        'revenue_q4_invalid'    : "Revenue should be up to 12 digit number Revenue Q4 (column L) || ",
        'fg_inventory_invalid'  : "Please insert a number between 1 to 52 indicating the number of weeks on average in finished goods inventory available (column M) || ",
        'revenue_q1_multiple'   : "First value will be updated(Revenue Q1 Multiple )if the same product has different Revenue Q1 (column G-I) || ",
        'revenue_q2_multiple'   : "First value will be updated(Revenue Q1 Multiple ) if the same product has different Revenue Q2 (column G-J) || ",
        'revenue_q3_multiple'   : "First value will be updated(Revenue Q1 Multiple ) if the same product has different Revenue Q3 (column G-K) || ",
        'revenue_q4_multiple'   : "First value will be updated(Revenue Q1 Multiple ) if the same product has different Revenue Q4 (column G-L) || ",
        'duplicate_product_tab' : "Product Hierarchy Level 1, Product Hierarchy Level 2, Product Hierarchy Level 3, Product Hierarchy Level 4, Product Hierarchy Level 5, Product Hierarchy Level 6 and Product Name should not be duplicate (column A, column B, column C, column D, column E, column F, column G) || ",
        'revenue_not_provided'  : "Product revenue will be loaded as $1 as the product revenue has not been provided || "
    };

    executiondetail_messages = {
        'cust_part_blank'               : "Please insert the part # (column A) || ",
        'cust_part_invalid'             : "Part# provided is not present in Column A of Tab 2 Part Sourcing (column A) || ",
        '3m_demand_invalid'             : "Please insert a number between 0 to 1000000000 (inclusive) for 3 month demand (column B) || ",
        '6m_demand_invalid'             : "Please insert a number between 0 to 1000000000 (inclusive) for 6 month demand (column C) || ",
        'inventory_onhand_invalid'      : "Please insert a number between 0 to 1000000000 (inclusive) for inventory on-hand (column D) || ",
        'inventory_onorder_invalid'     : "Please insert a number between 0 to 1000000000 (inclusive) for inventory on-order (column E) || ",
        'reduced_lead_time_invalid'     : "Please insert a number between 1 to 51 (inclusive) for reduced lead time and reduced leadtime should be less than full lead time (column F) || ",
        'full_lead_time_invalid'        : "Please insert a number between 1 to 52 (inclusive) for full lead time (column G) || ",
        'reduce_lead_less_full_lead'    : "Per industry recommendations, reduced lead time should be less than full lead time (column F) || ",
        'part_custom_defined_risk1_invalid': "Please enter a value between 1 to 10 for company part custom defined risk score 1 (column I) || ",
        'part_custom_defined_risk2_invalid': "Please enter a value between 1 to 10 for company part custom defined risk score 2 (column J) || ",
        'part_custom_defined_risk3_invalid': "Please enter a value between 1 to 10 for company part custom defined risk score 3 (column K) || ",
        'part_custom_defined_risk4_invalid': "Please enter a value between 1 to 10 for company part custom defined risk score 4 (column L) || ",
        'part_custom_defined_risk5_invalid': "Please enter a value between 1 to 10 for company part custom defined risk score 5 (column M) || ",
        'duplicate_part'       : "The part # should not be duplicate (column A) (If different risk scores are provided, then maximum value of risk score will be uploaded) || "
    }

    sitelist_messages = {
        'site_number_blank'                     : "Please insert the Site Number (column A) || ",
        'site_number_invalid'                   : "Site Number should be alphanumeric (Column A) || ",
        'same_site_number_for_diff_sites'       : "Please check the site number as it is provided for two or more different sites (column A), The site Number is: {0} || ",
        'diff_site_number_for_same_site'        : "Please check, different site numbers are provided for the same site (column A). The site numbers are: {0} || ",
        'site_numbers_with_same_duns'           : "Please check the site DUNS numbers as it seems there are two or more sites having same DUNS Number (column A), The site numbers are: {0} || ",
        'street_address1_blank'                 : "Please insert the street address (column B) || ",
        'city_blank'                            : "Please insert the city (column D) || " ,
        'state_province_blank'                  : "Please insert the state/province (column E) || ",
        'country_blank'                         : "Please insert the country (column G) || ",
        #'site_ownership_invalid'                : "Please choose the  Site Ownership field amongst 'Self-Owned' or 'CM Owned' or 'Partner-Owned' or 'Subcontractor Owned' (column H)",
        'site_activity_blank'                   : "Please provide site activity if recovery time has been provided (column I) || ",
        'site_activity_invalid'                 : "Please choose Site Activity from the drop-down (column I) || ",
        'recovery_time_blank'                   : "Please provide recovery time between 1 to 52 (weeks) if site activity has been provided (column J) || ",
        'recovery_time_invalid'                 : "Please enter an whole number between 1 to 52 (weeks) for recovery time (Column J) || ",
        'altsite_blank'                         : "Please input alternate site number if the alternate site bring up time has been provided (column I) || ",
        'altsite_primarysite_same'              : "Please ensure the alternate site is different from the primary site (Column G) || ",
        'alt_site_absent'                       : "An alternate site number should be present in site number column A (column K) || ",
        'partner_different'                     : "Please ensure that the primary and alternate site belong to the same partner (column K) || ",
        'altsite_bringuptime_invalid'           : "Please ensure alternate site bring up time is a whole number between 1 to 51 (Weeks) and less than recovery time(column L) || ",
        'altsite_bringuptime_blank'             : "Please ensure alternate site bring up time is a whole number between 1 to 51 ( weeks) if alternate site number has been provided (column L) || ",
        'bringuptime_greater_recoverytime'      : "Please ensure alternate site bring up time is a whole number between 1 to 51 (Weeks) and less than recovery time(column L) || ",
        'partner_name_blank'                    : "Please insert the Partner name (column M) || ",
        # TENAR-23557: Remove partner name not present in Partner tab validation wherever present and add those partners in Partner tab instead
        #'partner_name_absent'                   : "A Partner Name provided here should always be a tier 1 partner in tab 1, column AH (column S)",
        'emergency_contact_name_blank'          : "Please provide the site emergency contact name if email has been provided (column P) || ",
        'emergency_contact_email_blank'         : "Please provide the site emergency contact email if name has been provided (column Q) || ",
        'emergency_contact_email_invalid'       : "Please check and correct- the site emergency contact email format is invalid (column Q) || ",
        'emergency_contact_name_email_required' : "Please provide the site emergency contact name and email if phone has been provided (column R) || ",
        'site_list_duplicate_warning'           : "Site number, street address 1, street address 2, city, state/ province, postal/ zip code, country, activity, recovery time, alternate site number, alternate site bring up time, partner name, subcontractor name, site duns number and site emergency contact- email should not be duplicate (column A, column B, column C, column D, column E, column F, column G, column I, column J, column K, column L, column M, column N, column O, column Q) || ",
        'invalid_duns'                          : "Please provide DUNS number up to 9 digit (column O) || "
    }

    productsitemap_messages = {
        'product_name_blank'                : "Please insert the product name (column A) || ",
        'product_name_absent'               : "Product name provided here should be present in \"Tab 4. Product\", column G. (Column A) || ",
        'alt_site_qualified_picklist'       : "Please choose Y or N from drop down for Alternate Site Qualified (column B) || ",
        'site_number_blank'                 : "Please enter the site number from \"Tab 6 Site List\" \"Column A\" and site must be of tier 1 partner  (column C) || ",
        'site_number_absent'                : "Please enter the site number from \"Tab 6 Site List\" \"Column A\" and site must be of tier 1 partner  (column C) || ",
        'site_number_tier1'                 : "Please enter the site number from \"Tab 6 Site List\" \"Column A\" and site must be of tier 1 partner  (column C) || ",
        'activity_blank'                    : "Please insert activity from drop down (column D) || ",
        'activity_picklist'                 : "Please insert activity from drop down (column D) || ",
        'recovery_time_blank'               : "Please input the recovery time between 1 to 52 (weeks) if the site activity has been provided (Column E) || ",
        'recovery_time_invalid'             : "Please insert a whole number between 1 to 52 (Weeks) for recovery time (column E) || ",
        'split_invalid'                     : "Please insert a whole number (between 0 to 100) for the revenue split % (column F) || ",
        'splitsum_invalid'                  : "Please ensure the split % sums up to 100 (column F) || ",
        'duplicate_product_site_activity'   : "Product name, site number, activity and recovery time should not be duplicate (column A, column C, column D, column E) || "
    }
    partsitemap_messages = {
        'partner_name_blank'                : "Please insert partner name (column A) || ",
        'partner_part_blank'                : "Please insert the partner part # (column B) || ",
        'site_number_blank'                 : "Please insert the site number of the Tier 1 partner from \"Tab 6 Site List\"  Column A (column D) || ",
        'site_number_absent'                : "Please insert the site number of the Tier 1 partner from \"Tab 6 Site List\"  Column A (column D) || ",
        'activity_blank'                    : "Please choose activity from the drop-down (column E) || ",
        'activity_picklist'                 : "Please choose activity from the drop-down (column E) || ",
        'recovery_time_blank'               : "Please insert the recovery time between 1 to 52 (weeks) if the site activity has been provided (column F) || ",
        'recovery_time_invalid'             : "Please insert the recovery time between 1 to 52 (weeks) if the site activity has been provided (column F) || ",
        'alt_site_number_absent'            : "Input alternate site number from Tab '6. Site List', column A (column G) || ",
        'altsite_primarysite_same'          : "Please ensure alternate site is different from the primary site (column G) || ",
        'altsite_bringuptime_blank'         : "Please ensure alternate site bring up time is a whole number between 1 to 51 ( weeks) if alternate site number has been provided (column H) || ",
        'altsite_blank'                     : "Please insert alternate site number if the alternate site bring up time has been provided (column G) || ",
        'altsite_bringuptime_invalid'       : "Please ensure alternate site bring up time  is a whole number between 1 to 51 (Weeks) and it should be less than recovery time (column H) || ",
        'bringuptime_greater_recoverytime'  : "Please insert the alternate site bring up time between 1 to 51 (Weeks) and it should be less than recovery time (column H) || ",
        'partner_name_absent'               : "Resilinc Partner Name provided here should be present in Tab '1. Partners', column A  (column A) || ",
        'partner_part_absent'               : "Partner Part # provided here should be present in \"Tab 2. Part Sourcing\", Column F (column B) || ",
        'partner_part_same'                 : "Please ensure the partner part # corresponds with its respective partner name (column B) || ",
        'partner_not_same_for_site'         : "Please ensure the site number corresponds with its respective partner name (column D) || ",
        'partner_not_same_for_alt_site'     : "Please ensure the partner name of the alternate site matches the partner name of the primary site (column D and column G ) || ",
        'duplicate_pname_ppart_snum'        : "The Site number, resilinc partner name, partner part, activity, recovery time, alternate site number, alternate site bring up time should not be duplicate (column D, column I, column B, column E,column G and column H) || "
    };

    contactinfo_messages = {
        'name_blank'                          : "Please insert the contact name (column A) || ",
        'email_blank'                         : "Please insert the contact email (column B) || ",
        'email_invalid'                       : "Please correct the format of the contact email (column B) || ",
        'duplicate_cust_contact_info_tab'     : "The name of contact and contact email address should not be duplicate (column A and column B) || ",
        'email_name_null_provided'            : "Please provide name of contact and contact email address if contact phone number has been provided (column A and column B) || ",
        'duplicate_email_diff_contact_tab'    : "Two different names of contacts are provided for same contact email address (column B) || "
    };

    sitemapping_messages = {
        'to_site_number_blank'      : "Provide \"To Site Number\" from \"Tab 6. Site List\", Column A (column B) || ",
        'to_site_number_absent'     : "Provide \"To Site Number\" from \"Tab 6. Site List\", Column A (column B) || ",
        'from_site_number_absent'   : "Provide \"From Site Number\" from \"Tab 6. Site List\", Column A (column A) || ",
        'from_site_number_blank'    : "Provide \"From Site Number\" from \"Tab 6. Site List\", Column A (column A) || ",
        'identical_fromsite_tosite' : "\"From Site Number\" and \"To Site Number\" should be different (column A-B) || ",
        'partner_part_absent'       : "\"Partner Part #\" provided here should be present in \"Tab 2. Part Sourcing\", Column F (column C) || ",
        'partner_part_invalid'      : "\"Partner Part #\" and \"From Site Number\" should correspond to the same partner || ",
        'duplicate_fromsite_tosite' : "\"From Site Number\" and \"To Site Number\" should not be duplicate (column A-B) || "
    };