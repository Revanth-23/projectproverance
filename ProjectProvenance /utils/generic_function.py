import re

"""
generic_ function for Partner tab 
"""
def is_valid_risk(val):
        if not is_float(val):
            return False
        risk = round(float(str(val).strip()),2)
        if risk >= 1 and risk <= 10:
            return True
        return False

def is_float(val):
    try:
        float(str(val));
        return True;
    except Exception as ex:
        return False


def is_valid_email(val):
        val = str(val).strip()
        if re.match(r"^(([a-zA-Z&]|[0-9])|([-]|[_]|[.]|[']))+[@](([a-zA-Z0-9])|([-])){1,63}([.](([a-zA-Z0-9-&'_]){2,63})+){1,6}$", val) != None:
            return True
        return False

def is_duns_valid(val):
    val=str(int(float(val))).strip()
    if re.match(r"^[a-zA-Z0-9]{0,15}$",val)==None:
        return False
    return True


