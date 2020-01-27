from config import CONFIG
import pymssql
import uuid
import numpy as np
import pandas as pd
import os

# Database constants
VENDOR = CONFIG["sql_vendor"][0]
AUTH_TYPE = CONFIG["sql_auth_type"][0]
DB_INSERT_LIMIT = CONFIG["sql_insert_limit"][0]
BULK_INSERT_FOLDER = CONFIG["sql_bulk_insert_folder"][0]
SCHEMA = 'dbo'
NUM_CONN_TRIES = 10  # The number of times the code tries to establish the SQL connection in case there is an error.


class SqlDb:
    __db_conn__ = None

    def __init__(self, server, db):
        self.server = server
        self.db = db
        self.__bulk_insert_folder = r"\\" + server + '\\' + BULK_INSERT_FOLDER
        self.__check_connection()  # Creates a SQL connection

    def run_query(self, query):
        self.__check_connection()
        cur = self.__db_conn__.cursor(as_dict=True)
        cur.execute(query)
        if cur.description is None:
            cur.close()
            return
        data = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
        cur.close()
        df = pd.DataFrame(data, columns=column_names)
        if df.shape == (1, 1) and df.iloc[0, 0] is None:
            return pd.DataFrame(None)
        return df

    def exec_sp(self, s_sp_name, *args):
        self.__check_connection()
        cur = self.__db_conn__.cursor(as_dict=True)
        # Put args in a tuple
        cur.callproc(s_sp_name, args)
        cur.nextset()
        # Read data returned by the cursor
        data = cur.fetchall()
        cur.close()
        return pd.DataFrame(data)

    def exec_stored_procedure(self, s_sp_name):
        query = 'EXEC ' + ','.join(s_sp_name)
        self.run_query(query)
        return

    def get_sp_names(self):
        get_sp_name_query = "  select s.name + " + "'.'" + \
                            " + p.name as sproc_name_with_schema from sys.procedures as p inner join sys.schemas as s on s.schema_id = p.schema_id"
        return self.run_query(get_sp_name_query)

    def select(self, table_name, *select_columns, **where_clause):
        self.__check_connection()
        if select_columns is None or not select_columns:
            select_columns = self.get_column_names(table_name)
        query = 'SELECT ' + ','.join(str(s) for s in select_columns) + ' FROM ' + table_name + self.construct_where_clause(**where_clause)
        cur = self.__db_conn__.cursor()
        cur.execute(query)
        if cur.description is None:
            cur.close()
            return
        data = cur.fetchall()
        cur.close()
        return pd.DataFrame.from_records(data, columns=select_columns)

    def insert(self, df, table_name):
        # Make sure columns of df are in the right order
        df.columns = [c.lower() for c in df.columns]
        list_columns = self.get_column_names(table_name)
        list_columns = [c.lower() for c in list_columns]
        df = df[list_columns]
        self.__bulk_insert(df, table_name)

    def __bulk_insert(self, df, table_name, sep='\t', encoding='utf8'):
        temp_file = self.__bulk_insert_folder + '\\' + str(uuid.uuid4())
        df.to_csv(temp_file, sep=sep, header=False, index=False, encoding=encoding, line_terminator="\n")
        insert_query = "BULK INSERT dbo." + table_name + " FROM " + "'" + temp_file + "'" + "WITH ( FIELDTERMINATOR='\\t', ROWTERMINATOR='\\n', " \
                                                                                            "MAXERRORS=1); "
        try:
            self.run_query(insert_query)
            os.remove(temp_file)
        except ValueError:
            print('Bulk insert failed')
            os.remove(temp_file)

    def create_index(self, index_name, table_name, columns=None, is_clustered_index=True):
        if is_clustered_index:
            query = 'CREATE CLUSTERED INDEX ' + index_name + ' ON ' + table_name + ' (' + ','.join(columns) + ')'
        else:  # non-clustered index
            query = 'CREATE INDEX ' + index_name + ' ON ' + table_name + ' (' + ','.join(columns) + ')'
        self.run_query(query)

    def drop_index(self, table_name, index_name):
        try:
            query = 'DROP INDEX ' + index_name + ' ON ' + table_name
            self.run_query(query)
            return
        except:
            pass
        try:
            self.drop_constraint(table_name, index_name)
        except:
            print('Index ' + index_name + ' on ' + table_name + ' cannot be dropped!')

    def drop_constraint(self, table_name, constraint_name):
        query = 'ALTER TABLE ' + table_name + ' DROP CONSTRAINT ' + constraint_name
        self.run_query(query)

    def is_table_exist(self, table_name):
        query = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{}'".format(table_name)
        df_table = self.run_query(query)
        return not df_table.empty

    def is_index_exist(self, table_name, index_name):
        query = "SELECT * FROM sys.indexes WHERE name='{}' AND object_id=OBJECT_ID('{}.{}')".format(index_name, SCHEMA, table_name)
        df_pk = self.run_query(query)
        return not df_pk.empty

    def get_column_names(self, table_name):
        query = "SELECT COLUMN_NAME FROM " + self.db + ".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA= '" + SCHEMA + \
                "' AND TABLE_NAME = '" + table_name + "' ORDER BY ORDINAL_POSITION ASC"
        df_column_names = self.run_query(query)
        return df_column_names["COLUMN_NAME"].tolist()

    def delete_table(self, table_name):
        query = "IF OBJECT_ID('dbo.{}', 'U') IS NOT NULL DROP TABLE dbo.{}".format(table_name, table_name)
        self.run_query(query)

    def set_identity_insert_on(self, table_name):
        query = 'SET IDENTITY_INSERT {} ON'.format(table_name)
        self.run_query(query)

    def set_identity_insert_off(self, table_name):
        query = 'SET IDENTITY_INSERT {} OFF'.format(table_name)
        self.run_query(query)

    @staticmethod
    def construct_where_clause(**kwargs):
        # TODO: Extend this function to support where clauses with 'in', '<', '>' etc.
        where_clause = ''
        for key in kwargs:
            value = kwargs[key]
            where_clause += key + ("='" + str(value) + "'" if value is not None and value is not 'NULL' else ' is NULL ') + ' AND '
        if where_clause is not '':
            where_clause = ' WHERE ' + where_clause[:-4]
        return where_clause

    @staticmethod
    def dtype_switcher(arg):
        dtype = type(arg)
        switcher = {
            str: "%s",
            int: "%d",
            float: '%f',
            np.int64: '%d',
            np.int32: '%d',
            np.int16: '%d',
            np.int8: '%d',
            np.int: '%d',
            np.float: '%f',
            np.float64: '%f',
            np.float32: '%f',
            np.float16: '%f',
        }
        return switcher.get(dtype, None)

    def __check_connection(self):
        """
        Checks the SQL connection. If the connection is None, it tries to re-connect to SQL.
        :return: True if connection is established.
        """
        if self.__db_conn__ is None:
            for i in range(NUM_CONN_TRIES):
                try:
                    self.__db_conn__ = pymssql.connect(server=self.server, database=self.db)
                    self.__db_conn__.autocommit(True)
                    break
                except pymssql.DatabaseError as error:  # Catch errors that are related to the util. A subclass of Error.
                    print('Trying to connect {}. time'.format(str(i + 1)))
                    print('Error / Exception: ', error.args)
                except pymssql.Error as error:  # Catch all other error exceptions.
                    print('Trying to connect {}. time'.format(str(i + 1)))
                    print('Error / Exception: ', error.args)
        return self.__db_conn__ is not None

    def __del__(self):
        if self.__db_conn__ is not None:
            self.__db_conn__.close()
