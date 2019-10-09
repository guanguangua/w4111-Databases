from src import SQLHelper
from src.BaseDataTable import BaseDataTable
import pymysql


class RDBDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns, commit=False):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        if not table_name:
            table_name = "lahman2019raw"
        if not key_columns:
            raise Exception("Key columns can't be empty")
        self.table_name = table_name
        self.connect_info = connect_info if connect_info else SQLHelper._get_default_connection()
        self.key_columns = key_columns
        self.commit = commit  # if testing, don't commit

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        template = {k: v for k, v in zip(self.key_columns, key_fields)}
        sql, args = SQLHelper.create_select(table_name=self.table_name, template=template, fields=field_list)
        res, data = SQLHelper.run_q(sql=sql, args=args, conn=self.connect_info, commit=self.commit)
        if not data:
            return None
        else:
            return data[0]

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
        sql, args = SQLHelper.create_select(table_name=self.table_name, template=template, fields=field_list)
        res, data = SQLHelper.run_q(sql=sql, args=args, conn=self.connect_info, commit=self.commit)
        if not data:
            return None
        else:
            return data[0]

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        template = {k: v for k, v in zip(self.key_columns, key_fields)}
        sql, args = SQLHelper.create_delete(table_name=self.table_name, template=template)
        res, data = SQLHelper.run_q(sql=sql, args=args, conn=self.connect_info, commit=self.commit)
        return res

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        sql, args = SQLHelper.create_delete(table_name=self.table_name, template=template)
        res, data = SQLHelper.run_q(sql=sql, args=args, conn=self.connect_info, commit=self.commit)
        return res

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        template = {k: v for k, v in zip(self.key_columns, key_fields)}
        sql, args = SQLHelper.create_update(table_name=self.table_name, template=template, new_values=new_values)
        res, data = SQLHelper.run_q(sql=sql, args=args, conn=self.connect_info, commit=self.commit)
        return res

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        sql, args = SQLHelper.create_update(table_name=self.table_name, template=template, new_values=new_values)
        res, data = SQLHelper.run_q(sql=sql, args=args, conn=self.connect_info, commit=self.commit)
        return res

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        sql, args = SQLHelper.create_insert(table_name=self.table_name, row=new_record)
        res, data = SQLHelper.run_q(sql=sql, args=args, conn=self.connect_info, commit=self.commit)
        return None

    def get_rows(self):
        return self._rows
