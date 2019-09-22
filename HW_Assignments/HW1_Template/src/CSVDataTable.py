
from src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import pandas as pd

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)

class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug
        }

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = dict.copy(rows)
        else:
            self._rows = []
            self._load()

    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0, CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1*temp_r)-1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)
            for r in csv_d_rdr:
                self._add_row(r)

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def save(self):
        """
        Write the information back to a file.
        :return: None
        """
        pass


    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

    # helper function
    def matches_key_field(self, row, key_fields):
        key_columns = self._data["key_columns"]
        for column, field in zip(key_columns, key_fields):
            if row[column] != field:
                return False
        return True

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        if not self._rows:
            return None
        # Exception handling
        key_columns = self._data["key_columns"]
        if not key_fields:
            raise Exception("key_fields can't be empty")
        if len(key_fields) != len(key_columns):
            raise Exception("key_fields have a different size as the primary keys")
        if field_list and not set(field_list).issubset(set(self._rows[0].keys())):
            raise Exception("Mismatch in key_fields and table columns")

        result = None
        for row in self._rows:
            if self.matches_key_field(row, key_fields):
                result = dict.copy(row)
        if not result:
            return None
        if field_list:
            result = {k : result[k] for k in field_list}
        return result

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        if not self._rows:
            return []
        # Exception handling
        if field_list and not set(field_list).issubset(set(self._rows[0].keys())):
            raise Exception("Mismatch in key_fields and table columns")
        if template and not set(template.keys()).issubset(set(self._rows[0].keys())):
            raise Exception("Mismatch in template keys and table columns")

        result = []
        for row in self._rows:
            if self.matches_template(row, template):
                if field_list:
                    row = {k : row[k] for k in field_list}
                result.append(dict.copy(row))
        return result

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param key_fields: A template.
        :return: A count of the rows deleted.
        """
        if not self._rows:
            return 0
        key_columns = self._data["key_columns"]
        if not key_fields:
            raise Exception("key_fields can't be empty")
        if len(key_fields) != len(key_columns):
            raise Exception("key_fields have a different size as the primary keys")

        count = 0
        for row in reversed(self._rows):
            if self.matches_key_field(row, key_fields):
                self._rows.remove(row)
                count += 1

        return count

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        key_columns = self._data["key_columns"]
        if not self._rows:
            return 0
        if template and not set(template.keys()).issubset(set(self._rows[0].keys())):
            raise Exception("Mismatch in template keys and table columns")

        count = 0
        for row in reversed(self._rows):
            if self.matches_template(row, template):
                self._rows.remove(row)
                count += 1

        return count

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        if not new_values or not self._rows:
            return 0
        key_columns = self._data["key_columns"]
        if not key_fields:
            raise Exception("key_fields can't be empty")
        if len(key_fields) != len(key_columns):
            raise Exception("key_fields have a different size as the primary keys")
        if new_values and not set(new_values.keys()).issubset(set(self._rows[0].keys())):
            raise Exception("Mismatch in new_values keys and table columns")

        count = 0
        key_columns = self._data["key_columns"]
        rows_to_update = {} # TODO
        for idx, row in enumerate(self._rows):
            if len(row) != len(key_fields):
                raise Exception("Data in loaded rows corrupted")
            if self.matches_key_field(row, key_fields):
                for k, v in new_values.items():
                    self._rows[idx][k] = v
                count += 1
        return count

    def update_by_template(self, template, new_values): # TODO
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        key_columns = self._data["key_columns"]
        if not self._rows:
            return 0
        if template and not set(template.keys()).issubset(set(self._rows[0].keys())):
            raise Exception("Mismatch in template keys and table columns")

        count = 0
        for idx, row in enumerate(self._rows):
            if self.matches_template(row, template):
                self._rows[idx] = dict.copy(new_values)
                count += 1
        return count


    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        key_columns = self._data["key_columns"]

        def same_key_field(r1, r2):
            if all(r1[col] == r2[col] for col in key_columns):
                return True
            return False

        for row in self._rows:
            if same_key_field(row, new_record):
                raise Exception("Can not insert row, same row exists")
        self._rows.append(dict.copy(new_record))
        return None

    def get_rows(self):
        return self._rows

