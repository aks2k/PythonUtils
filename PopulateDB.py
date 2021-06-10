# coding: utf-8
import mysql.connector
import traceback
from config import *
from LogUtil import *


class PopulateDB:

    def __init__(self):
        connection_args = {
            'host': HOST,
            'database': DATABASE,
            'user': USER,
            'password': PASSWD,
            'port': PORT
        }
        self.db = mysql.connector.connect(**connection_args)
        self.log = get_logger()
        try:
            self.cursor = self.db.cursor()
        except:
            print("Cannot connect to DB")
            traceback.print_exc()
            return None

    def query(self, sql):
        try:
            self.cursor.execute(sql)
        except:
            print("Error in executing SQL:" + sql)
            traceback.print_exc()
            return None
        return self.cursor.fetchall()

    def query_insert_update(self, sql):
        try:
            self.cursor.execute(sql)
        except:
            self.log.debug("Error in executing SQL:" + sql)
            traceback.print_exc()
            return None
        self.db.commit()
        return self.cursor.rowcount

    def rows(self):
        return self.cursor.rowcount

    def insert(self, table_name, data_dict):
        data_dict_len = len(data_dict)
        data_dict_idx = 0
        data_key_str = ''
        data_val_str = ''
        for data_key, data_val in data_dict.items():
            if data_dict_idx == 0:
                data_key_str = " ("
                data_val_str = " ("
            if 0 < data_dict_idx < data_dict_len:
                data_key_str = data_key_str + ","
                data_val_str = data_val_str + ","
            data_key_str = data_key_str + str(data_key)
            if data_key == 'INSERT_DATE_TIME':
                data_val_str = data_val_str + "NOW()"
            else:
                data_val_str = data_val_str + "'" + str(data_val) + "'"
            if data_dict_idx == data_dict_len - 1:
                data_key_str = data_key_str + ")"
                data_val_str = data_val_str + ")"
            data_dict_idx += 1

        sql = "insert into " + table_name + data_key_str + " values " + data_val_str
        result = self.query_insert_update(sql)
        if result is None:
            self.log.debug("insert failed:" + sql)
        else:
            self.log.debug("insert successful in :" + table_name)
        return result

    def update(self, table_name, where_dict, data_dict):
        data_dict_len = len(data_dict)
        data_dict_idx = 0
        data_str = ''

        for data_key, data_val in data_dict.items():
            if data_key == 'UPDATE_DATE_TIME':
                data_str = data_str + ' ' + str(data_key) + ' = ' + "NOW()"
            else:
                data_str = data_str + ' ' + str(data_key) + ' = ' + "'" + str(data_val) + "'"
            if data_dict_idx < data_dict_len - 1:
                data_str = data_str + ","
            data_dict_idx += 1

        where_dict_len = len(where_dict)
        where_str = ''
        where_idx = 0
        for where_key, where_val in where_dict.items():
            if 0 < where_idx < where_dict_len:
                where_str = where_str + " AND "
            where_str = where_str + ' ' + str(where_key) + "='" + str(where_val) + "'"
            where_idx += 1

        sql = 'update ' + table_name + ' set ' + data_str + ' where ' + where_str
        result = self.query_insert_update(sql)
        if result is None:
            self.log.debug("update failed:" + sql)
        else:
            self.log.debug("update successful in :" + table_name)

        return result

    def check_insert_update(self, table_name, identifier, where_dict, data_dict):
        where_dict_len = len(where_dict)
        where_str = ''
        where_idx = 0
        for where_key, where_val in where_dict.items():
            if 0 < where_idx < where_dict_len:
                where_str = where_str + " AND "
            where_str = where_str + ' ' + str(where_key) + "='" + str(where_val) + "'"
            where_idx += 1

        if identifier is None:
            sql = 'select 1 from ' + table_name + ' where ' + where_str
            id_val = 1
        else:
            sql = 'select identifier from ' + table_name + ' where ' + where_str

        result = self.query(sql)
        if result is None:
            self.log.debug('select identifier query failed:' + sql)
            return None
        matched_row_count = self.rows()

        if matched_row_count == 0:
            if identifier is not None:
                sql = "select IFNULL(max(" + identifier + "),0) from " + table_name
                result = self.query(sql)
                if result is None:
                    self.log.debug('max identifier query failed:' + sql)
                    return None
                id_val = result[0][0] + 1
                data_dict[identifier] = id_val
            self.insert(table_name, data_dict)
        elif matched_row_count == 1:
            id_val = result[0][0]
            if 'INSERT_DATE_TIME' in data_dict:
                del data_dict['INSERT_DATE_TIME']
                data_dict['UPDATE_DATE_TIME'] = "NOW()"
            result = self.update(table_name, where_dict, data_dict)
        else:
            self.log.debug("Found more than 1 match:" + sql)
            return None
        return id_val
