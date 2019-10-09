from src.RDBDataTable import RDBDataTable
import logging
import os
import src.SQLHelper as SQLHelper

# The logging level to use should be an environment variable, not hard coded.
#logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
#logger = logging.getLogger()
#logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")


def t_rdb_find_by_primary_key():
    connect_info = SQLHelper._get_default_connection()
    rdb_tbl = RDBDataTable("people", connect_info, ["playerID"], commit=False)
    r = {
        "playerID": "abbotgl01"
    }
    print(rdb_tbl.find_by_primary_key(r.values()))
    print(rdb_tbl.find_by_primary_key(r.values(), ["birthCity", "birthState"]))


def t_rdb_find_by_template():
    connect_info = SQLHelper._get_default_connection()
    rdb_tbl = RDBDataTable("people", connect_info, ["playerID"], commit=False)
    r = {
        "playerID": "aasedo01",
        "birthYear": "1954",
        "birthCity": "Orange"
    }
    # print(rdb_tbl.find_by_template({}))
    print(rdb_tbl.find_by_template(r))
    print(rdb_tbl.find_by_template(r, ["weight", "height"]))
    print(rdb_tbl.find_by_template(r, ["deathYear"]))


def t_rdb_delete_by_key():
    connect_info = SQLHelper._get_default_connection()
    rdb_tbl = RDBDataTable("people", connect_info, ["playerID"], commit=False)

    print("Lines removed:", rdb_tbl.delete_by_key(["abbotgl01"]), "should equal 1")
    print("Lines removed:", rdb_tbl.delete_by_key(["abbotgl01"]), "should equal 0")
    print("Lines removed:", rdb_tbl.delete_by_key(["abbeych01"]), "should equal 1")


def t_rdb_delete_by_template():
    connect_info = SQLHelper._get_default_connection()
    rdb_tbl = RDBDataTable("people", connect_info, ["playerID"], commit=False)
    r = {
        "playerID": "abbotgl01",
        "birthYear": "1951",
        "birthMonth": "2"
    }
    print("Lines removed:", rdb_tbl.delete_by_template(r), "should equal 1")
    print("Lines removed:", rdb_tbl.delete_by_template(r), "should equal 0")
    print("Lines removed:", rdb_tbl.delete_by_template({"birthYear": "1951"}), "should > 1")
    print("Lines removed:", rdb_tbl.delete_by_template({"birthYear": "1951"}), "should equal 0")


def t_rdb_update_by_key():
    connect_info = SQLHelper._get_default_connection()
    rdb_tbl = RDBDataTable("people", connect_info, ["playerID"], commit=False)

    keys = ["abbotgl01"]
    new_val = {"deathYear": "123", "deathMonth": "12", "deathDay": "1"}
    print("Lines updated:", rdb_tbl.update_by_key(keys, new_val), "should equal 1")
    print("Lines updated:", rdb_tbl.update_by_key(keys, new_val), "should equal 0")
    print("Lines updated:", rdb_tbl.update_by_key(keys, {"playerID": "abbotgl01"}), "should equal 0")

def t_rdb_update_by_template():
    connect_info = SQLHelper._get_default_connection()
    rdb_tbl = RDBDataTable("people", connect_info, ["playerID"], commit=False)
    r = {
        "playerID": "abbotgl01"
    }
    new_val = {"deathYear": "123", "deathMonth": "12", "deathDay": "1"}
    print("Lines updated:", rdb_tbl.update_by_template(r, new_val), "should equal 1")
    print("Lines updated:", rdb_tbl.update_by_template(r, new_val), "should equal 0")
    print("Lines updated:", rdb_tbl.update_by_template(r, {"playerID": "abbotgl01"}), "should equal 0")

def t_rdb_insert():
    connect_info = SQLHelper._get_default_connection()
    rdb_tbl = RDBDataTable("people", connect_info, ["playerID"], commit=False)
    rdb_tbl.insert({"playerID": "abbotgl99"})
    if not rdb_tbl.find_by_primary_key(["abbotgl99"]):
        print("insert test, failed")
    else:
        print("insert test, passed")

t_rdb_find_by_primary_key()
t_rdb_find_by_template()
t_rdb_delete_by_key()
t_rdb_delete_by_template()
t_rdb_update_by_key()
t_rdb_update_by_template()
t_rdb_insert()