from src.CSVDataTable import CSVDataTable
import logging
import os

# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")


def t_load():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    print("Created table = " + str(csv_tbl))


def t_csv_find_by_primary_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])

    r = {
        "playerID": "abbotgl01"
    }
    print(csv_tbl.find_by_primary_key(r.values()))
    print(csv_tbl.find_by_primary_key(r.values(), ["birthCity", "birthState"]))

    try:
        csv_tbl.find_by_primary_key(None)
        print("should give empty keys exception, failed")
    except Exception:
        print("None test, passed")
    try:
        csv_tbl.find_by_primary_key(list(r.values()) + ["birthCity"])
        print("should give different size exception, failed")
    except Exception:
        print("key_fields have a different size as the primary keys, passed")
    try:
        csv_tbl.find_by_primary_key(list(r.values()), ["123"])
        print("Mismatch in key_fields and table column exception, failed")
    except Exception:
        print("Mismatch in key_fields and table column exception, passed")


def t_csv_find_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])

    r = {
        "playerID": "aasedo01",
        "birthYear": "1954",
        "birthCity": "Orange"
    }
    print(csv_tbl.find_by_template({})[:10])
    print(csv_tbl.find_by_template(r))
    print(csv_tbl.find_by_template(r, ["weight", "height"]))
    print(csv_tbl.find_by_template(r, ["deathYear"]))
    try:
        csv_tbl.find_by_template(r, ["weight", "height", "luhuhyg"])
        print("field list mismatch, failed")
    except Exception:
        print("field list mismatch, passed")

    try:
        temp_r = {
            "fdsafds": "aasedo01",
            "birthYear": "1954",
            "birthCity": "Orange"
        }
        csv_tbl.find_by_template(temp_r)
        print("Column mismatch, failed")
    except Exception:
        print("Column mismatch, passed")


def t_csv_delete_by_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])

    r1 = {
        "playerID": "abbotgl01"
    }
    print("Lines removed:", csv_tbl.delete_by_key(["abbotgl01"]), "should equal 1")
    print("Lines removed:", csv_tbl.delete_by_key(["abbotgl01"]), "should equal 0")
    print("Lines removed:", csv_tbl.delete_by_key(["abbeych01"]), "should equal 1")
    try:
        csv_tbl.delete_by_key(None)
        print("None test, failed")
    except Exception:
        print("None test, passed")

    try:
        csv_tbl.delete_by_key(["abbeych01", "1866", "10", "sdfafdsa"])
        print("field-size test, failed")
    except Exception:
        print("field-size test, passed")


def t_csv_delete_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])

    r = {
        "playerID": "abbotgl01",
        "birthYear": "1951",
        "birthMonth": "2"
    }
    print("Lines removed:", csv_tbl.delete_by_template(r), "should equal 1")
    print("Lines removed:", csv_tbl.delete_by_template(r), "should equal 0")
    print("Lines removed:", csv_tbl.delete_by_template({"birthYear": "1951"}), "should > 1")
    print("Lines removed:", csv_tbl.delete_by_template({"birthYear": "1951"}), "should equal 0")
    try:
        csv_tbl.delete_by_template({"fdsfdafd": "123"})
        print("Mismatch test failed")
    except Exception:
        print("Mismatch test passed")


def t_csv_update_by_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])

    r = {
        "playerID": "abbotgl01"
    }
    keys = ["abbotgl01"]
    new_val = {"deathYear": "123", "deathMonth": "12", "deathDay": "1"}
    print("Lines updated:", csv_tbl.update_by_key(keys, new_val), "should equal 1")
    print("Lines updated:", csv_tbl.update_by_key(keys, new_val), "should equal 0")
    print("Lines updated:", csv_tbl.update_by_key(keys, {"playerID": "abbotgl01"}), "should equal 0")
    # try:
    #     csv_tbl.update_by_key(["abbotgl01"], {"playerID": "aaronha01"})
    #     print("Duplicate primary key update test, failed")
    # except Exception:
    #     print("Duplicate primary key update test, passed")


def t_csv_update_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])

    r = {
        "playerID": "abbotgl01"
    }
    new_val = {"deathYear": "123", "deathMonth": "12", "deathDay": "1"}
    print("Lines updated:", csv_tbl.update_by_template(r, new_val), "should equal 1")
    print("Lines updated:", csv_tbl.update_by_template(r, new_val), "should equal 0")
    print("Lines updated:", csv_tbl.update_by_template(r, {"playerID": "abbotgl01"}), "should equal 0")
    # try:
    #     csv_tbl.update_by_template(r, {"playerID": "aaronha01"})
    #     print("Duplicate primary key update test, failed")
    # except Exception:
    #     print("Duplicate primary key update test, passed")


def t_csv_insert():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])

    r = {
        "playerID": "abbotgl01"
    }

    csv_tbl.insert({"playerID": "abbotgl99"})
    print(csv_tbl.find_by_primary_key(["abbotgl99"]))
    try:
        csv_tbl.insert({"playerID": "abbotgl99"})
        print("Duplicate row insert test, failed")
    except Exception:
        print("Duplicate row insert test, passed")

    try:
        csv_tbl.insert({"birthYear": "abbotgl99"})
        print("Empty key insert test, failed")
    except Exception:
        print("Empty key insert test, passed")


# t_load()
t_csv_find_by_primary_key()
t_csv_find_by_template()
t_csv_delete_by_key()
t_csv_delete_by_template()
t_csv_update_by_key()
t_csv_update_by_template()
t_csv_insert()
