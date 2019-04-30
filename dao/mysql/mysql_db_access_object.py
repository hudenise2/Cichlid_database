import sys
import pymysql
import logging

__author__ = 'Maxim Scheremetjew, EMBL-EBI'
__author__ = 'Simon Potter, EMBL-EBI'

"""
Created on 07/12/2015

"""


class MySQLDataAccessObject:
    """Database access object"""

    def __init__(self, dbConnection):
        """
        Constructor
        """
        self.dbConnection = dbConnection

    def _runQuery(self, query):
        """Runs the query"""
        cursor = self.dbConnection.cursor()
        cursor.execute(query)
        results = []
        result = cursor.fetchone()
        while result is not None:
            numCols = len(result)
            item = {}
            for col in range(0, numCols):
                key = cursor.description[col][0]
                value = None
                if hasattr(result[key], 'read'):
                    # handle oracle clob datatypes
                    try:
                        value = result[key].read()
                    except AttributeError as e:
                        if hasattr(e, 'reason'):
                            logging.error(e.reason, sys.exc_info()[0])
                        elif hasattr(e, 'code'):
                            logging.error('Error code: ', e.code)
                        raise
                    except:
                        logging.error("Unexpected error:", sys.exc_info()[0])
                        raise
                else:
                    value = result[key]
                item[key] = value
                # item = {cursor.description[col][0]: result[col] for col in range(0, numCols)}
            results.append(item)
            result = cursor.fetchone()
        return results

    def _runInsert(self, table, columns, rows):
        try:
            """Runs the given insert statement"""
            logging.debug("Running insert statement for table " + table)
            cursor = self.dbConnection.cursor()
            sql = "INSERT INTO " + table + " (" + ",".join(columns) + ") values (" + ("%s," * len(columns))[:-1] + ")"
            for row in rows:
                for i in range(len(row)):
                    if row[i] == 'null':
                        row[i] = None
                cursor.execute(sql, row)
        except UnicodeEncodeError as unicode_error:
            logging.error("Encoding error: ", unicode_error.reason, sys.exc_info()[0])
            raise
        except pymysql.err.IntegrityError as e:
            logging.error("MySQL integrity error. An entry with that primary key constraint does already exist.", \
                          sys.exc_info()[0])
            raise
        except:
            logging.error("Unexpected error: ", sys.exc_info()[0])
            raise

    def _runUpdate(self, table, rows):
        """
        :parameter table: type str
        :parameter rows: type list(list)

        runUpdate(self, table, columns, rows) -> None

        Runs the given insert statement
        """
        try:
            logging.debug("Running update statements for table " + table)
            cursor = self.dbConnection.cursor()
            for row in rows:
                column_name = row[0]
                sql = "UPDATE " + table + " SET " + column_name + " = %s where sample_id = %s"
                for i in range(len(row)):
                    if row[i] == 'null':
                        row[i] = None
                # Remove column name from the list (which is the first element)
                row.pop(0)
                cursor.execute(sql, row)
        except UnicodeEncodeError as unicode_error:
            logging.error("Encoding error: ", unicode_error.reason, sys.exc_info()[0])
            raise
        except pymysql.err.IntegrityError as e:
            logging.error("MySQL integrity error. An entry with that primary key constraint does already exist.", \
                          sys.exc_info()[0])
            raise
        except:
            logging.error("Unexpected error: ", sys.exc_info()[0])
            raise

    def _runInsertStatement(self, insert_stmt, data=None):
        try:
            """Runs the given insert statement"""
            logging.debug("Running the following insert statement: " + insert_stmt)
            cursor = self.dbConnection.cursor()
            if data:
                cursor.execute(insert_stmt, data)
            else:
                cursor.execute(insert_stmt)
        except UnicodeEncodeError as unicode_error:
            logging.error("Encoding error: ", unicode_error.reason, sys.exc_info()[0])
            raise
        except pymysql.err.IntegrityError as e:
            logging.error("MySQL integrity error. An entry with that primary key constraint does already exist.", \
                          sys.exc_info()[0])
            raise
        except:
            logging.error("Unexpected error: ", sys.exc_info()[0])
            raise


if __name__ == '__main__':
    pass
