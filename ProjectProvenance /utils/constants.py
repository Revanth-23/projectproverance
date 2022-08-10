import re


class CDIF:
    def __init__(self, LOGGER=None):
        CDIF_DIR = "files"
        CDIF_FILE_NAME = "test"
        CDIF_FILE_EXT = ".xlsx"

        CDIF_TABS_DICT = {
            "1. Partners": {
                "alternate_tab": "1. Suppliers",
                "required": True,
                "compulsory_columns": [
                    "Partner Name",
                    "Partner Type"
                    "Partner Contact- Name",
                    "Partner Contact-  Email",
                    "Partner Emergency Contact- Name",
                    "Partner Emergency Contact- Email",
                    "Tier Level",
                    "Higher Tier Partner",
                ],
                "duplicates_subset": [
                    "Partner Contact-  Email",
                    "Partner Emergency Contact- Email",
                ],
                # Revanth
            },
            "2. Part sourcing": {
                "alternate_tab": None,
                "required": True,
                "compulsory_columns": ["Part #", "Partner Name", "Partner Part #"],
                "duplicates_subset": ['Part #','Description','Partner Part #','Annual Spend on Partner Part'],
                # Rajveer
            },
            "3. BOM and Alt Parts": {
                "alternate_tab": "3. BOM",
                "required": True,
                "compulsory_columns": ["Product name", "Part #"],
                "duplicates_subset": ["Product name", "Part #"],
                # Athul
            },
            "4. Product": {
                "alternate_tab": None,
                "required": True,
                "compulsory_columns": ["Product Name"],
                "duplicates_subset": [
                    "Product Hierarchy Level 1",
                    "Product Hierarchy Level 2",
                    "Product Hierarchy Level 3",
                    "Product Hierarchy Level 4",
                    "Product Hierarchy Level 5",
                    "Product Hierarchy Level 6",
                ], 
                 # Subash
            },
            "5. Executional Details": {
                "alternate_tab": None,
                "required": True,
                "compulsory_columns": ["Part #"],
                "duplicates_subset": ["Part #"], 
                #Avinash
            },
            "6. Site List": {
                "alternate_tab": None,
                "required": True,
                "compulsory_columns": [
                    "Site Number",
                    "Street Address 1",
                    "City",
                    "State/ Province",
                    "Country",
                    "Partner Name",
                    "Site Emergency Contact- Name",
                    "Site Emergency Contact- Email",
                    "Alternate Site Number",
                    "Alternate Site Bring Up Time",
                    "Activity",
                    "Recovery Time",
                ],
                "duplicates_subset": [
                    "Site Number",
                    "Activity",
                    "Alternate Site Number",
                    "Recovery Time",
                    "Alternate Site Bring Up Time",
                    "Street Address 1",
                    "Street Address 2",
                    "City",
                    "State/ Province",
                    "Postal/ Zip Code",
                    "Country",
                    "Site DUNS Number",
                    "Subcontractor Name",
                    "Site Emergency Contact- Email",
                ], 
                #Anchal
            },
            "7. Product - Site Map": {
                "alternate_tab": None,
                "required": True,
                "compulsory_columns": [
                    "Product Name",
                    "Site Number",
                    "Activity",
                    "Recovery Time",
                ],
                "duplicates_subset": [
                    "Product Name",
                    "Site Number",
                    "Activity",
                    "Recovery Time",
                ], 
                # Dheeraj 
            },
            "8. Part - Site Map": {
                "alternate_tab": None,
                "required": True,
                "compulsory_columns": [
                    "Partner Name",
                    "Partner Part #",
                    "Site Number",
                    "Activity",
                    "Recovery Time",
                ],
                "duplicates_subset": [
                    "Partner Name",
                    "Partner Part #",
                    "Site Number",
                    "Activity",
                    "Recovery Time",
                ],# padmini
            },
            "9. Customer Contact Information": {
                "alternate_tab": None,
                "required": False,
                "compulsory_columns": [
                    "Name of Contact",
                    "Contact Email Address",
                ], #Sujith
            },
            "11. Site to Site Mapping": {
                "alternate_tab": None,
                "required": True,
                "compulsory_columns": ["From Site Number", "To Site Number"],
                "duplicates_subset":['From Site Number', 'To Site Number']
            }, #Sujith
        }
        CDIF_ALT_TABS_DICT = {
            "1. Suppliers": {
                "compulsory_columns": [
                    "Supplier Name",
                    "Supplier Contact- Name",
                    "Supplier Contact-  Email",
                    "Supplier Emergency Contact- Name",
                    "Supplier Emergency Contact- Email",
                    "Tier Level",
                    "Higher Tier Supplier",
                ],
            },
            "3. BOM": {
                "compulsory_columns": ["Product name", "Part #"],
            },
        }
        self.CDIF_TABS_DICT = CDIF_TABS_DICT
        self.CDIF_ALT_TABS_DICT = CDIF_ALT_TABS_DICT
        self.CDIF_DIR = CDIF_DIR
        self.CDIF_FILE_NAME = CDIF_FILE_NAME
        self.CDIF_FILE_EXT = CDIF_FILE_EXT
        self.LOGGER = LOGGER

    def shortened_tab_name(self, tab):
        reg_exp1 = "^\d+. "
        reg_exp2 = "s$"
        tab = re.sub(reg_exp1, "", tab)
        tab = re.sub(reg_exp2, "", tab)
        tab = tab.replace(" ", "_").replace("-", "").replace("__", "_").lower()
        return tab

    def alternate_supplier_tab_headers(self, headers):
        alternate_headers = []
        for header in headers:
            if header == "Supplier Category":
                header = "Category"
            header = header.replace("Supplier", "Partner")
            alternate_headers.append(header)

        return alternate_headers

    def missing_headers(self, tabs, headers):
        tab, reference_tab = tabs

        if tab == reference_tab:
            compulsory_cols = self.CDIF_TABS_DICT.get(tab).get("compulsory_columns")
        else:
            compulsory_cols = self.CDIF_ALT_TABS_DICT.get(tab).get("compulsory_columns")
        return list(set(compulsory_cols) - set(headers))

    def alternate_headers(self, tabs=None, headers=None):
        tab, reference_tab = tabs
        if tab != reference_tab:
            shortened_tab_name = self.shortened_tab_name(tab)
            header_function = f"alternate_{shortened_tab_name}_tab_headers"
            if hasattr(eval(self.__class__.__name__), header_function):
                return eval(f"self.{header_function}({headers})")
        return None

    def duplicate_subset(self, tabs=None, headers=None):
        tab, reference_tab = tabs
        duplicate_subset = self.CDIF_TABS_DICT.get(reference_tab).get(
            "duplicate_subset", None
        )
        if duplicate_subset:
            return list(set(headers).intersection(set(duplicate_subset)))
        return None


# class Constants:
#     CDIF_DIR = "files"
#     CDIF_FILE_NAME = "test"
#     CDIF_FILE_EXT = ".xlsx"

#     CDIF_TABS_DICT = {
#         "1. Partners": {
#             "alternate_tab": "1. Suppliers",
#             "required": True,
#             "compulsory_columns": [
#                 "Partner Name",
#                 "Partner Contact- Name",
#                 "Partner Contact-  Email",
#                 "Partner Emergency Contact- Name",
#                 "Partner Emergency Contact- Email",
#                 "Tier Level",
#                 "Higher Tier Partner",
#             ],
#         },
#         "2. Part sourcing": {
#             "alternate_tab": None,
#             "required": True,
#             "compulsory_columns": ["Part #", "Partner Name", "Partner Part #"],
#         },
#         "3. BOM and Alt Parts": {
#             "alternate_tab": "BOM",
#             "required": True,
#             "compulsory_columns": ["Product name", "Part #"],
#         },
#         "4. Product": {
#             "alternate_tab": None,
#             "required": True,
#             "compulsory_columns": ["Product Name"],
#         },
#         "5. Executional Details": {
#             "alternate_tab": None,
#             "required": True,
#             "duplicates_subset": ["3M Demand", "6M Demand"],
#             "compulsory_columns": ["Part #"],
#         },
#         "6. Site List": {
#             "alternate_tab": None,
#             "required": True,
#             "compulsory_columns": [
#                 "Site Number",
#                 "Street Address 1",
#                 "City",
#                 "State/ Province",
#                 "Country",
#                 "Partner Name",
#                 "Site Emergency Contact- Name",
#                 "Site Emergency Contact- Email",
#                 "Alternate Site Number",
#                 "Alternate Site Bring Up Time",
#                 "Activity",
#                 "Recovery Time",
#             ],
#         },
#         "7. Product - Site Map": {
#             "alternate_tab": None,
#             "required": True,
#             "compulsory_columns": [
#                 "Product Name",
#                 "Site Number",
#                 "Activity",
#                 "Recovery Time",
#             ],
#         },
#         "8. Part - Site Map": {
#             "alternate_tab": None,
#             "required": True,
#             "compulsory_columns": [
#                 "Partner Name",
#                 "Partner Part #",
#                 "Site Number",
#                 "Activity",
#                 "Recovery Time",
#                 "Alternate Site Number",
#                 "Alternate Site Bring Up Time",
#             ],
#         },
#         "9. Customer Contact Information": {
#             "alternate_tab": None,
#             "required": False,
#             "compulsory_columns": [
#                 "Name of Contact",
#                 "Contact Email Address",
#             ],
#         },
#         "11. Site to Site Mapping": {
#             "alternate_tab": None,
#             "required": True,
#             "compulsory_columns": ["From Site Number", "To Site Number"],
#         },
#     }
