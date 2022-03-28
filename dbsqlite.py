import sqlite3
import logging


class DbSqlite:
    """
    A class to handle SQLite Database. The code is using sqlite3 library.
    """

    def __init__(self, db_name):
        self.conn = None
        self.db_name = db_name

    def db_connect(self):

        try:
            self.conn = sqlite3.Connection(self.db_name)
            self.cur = self.conn.cursor()

        except Exception as err:
            logging.error("connection to db failed" + str(err))
            raise err

    def execute_sql(self, query_str, params_dict=None):
        """
        Executes the received query
        :param sql_str: A string contains the sql query
        :param params_dict: A dict contains the parameters required in sql_str
        :return: List of records if query returns a description else None
        """
        if params_dict:
            self.cur.execute(query_str, params_dict)
        else:
            self.cur.execute(query_str)
        if self.cur.description:
            return self.cur.fetchall()
        else:
            return None

    def execute_many(self, query_str, params_list):
        self.cur.executemany(query_str, params_list)

    def db_commit(self):
        """
        Sends a COMMIT statement to the database to commit all the statements within the current transaction.
        """
        self.conn.commit()

    def db_rollback(self):
        """
        Sends a ROLLBACK statement to the database to ROLLBACK all the statements within the current transaction.
        """
        self.conn.rollback()

    def db_close(self):
        """
        Closes the current connection
        """
        try:
            self.conn.close()
        # pylint: disable=broad-except
        except Exception:
            pass
