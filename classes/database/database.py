import mysql.connector


class UpdateDB:

    def __init__(self, con, logger):
        self._con = con
        self._logger = logger

    @property
    def con(self):
        return self._con

    @property
    def logger(self):
        return self._logger

    def update(self, query, param=[]):
        try:
            cur = self.con.cursor()
            cur.execute(query, param)

            self.con.commit()

            return "Success"
        except Exception as e:
            self.logger.error("An error has occurred while updating db.")
            self.logger.error(f"Error: {e}")
            return e

    # def insert(self, query):
    #     try:
    #         cur = self.con.cursor()
    #         print(query)
    #         cur.execute(query)

    #         self.con.commit()
    #         return "Success"
    #     except Exception as e:
    #         self.logger.error("An error has occurred while updating db.")
    #         self.logger.error(f"Error: {e}")
    #         return e
    def executemany(self, sql, args):
        """
        Execute with many args. Similar with executemany() function in pymysql.
        args should be a sequence.
        :param sql: sql clause
        :param args: args
        :param commit: commit or not.
        :return: if commit, return None, else, return result
        """
        # get connection form connection pool instead of create one.

        cur = self.con.cursor()
        cur.executemany(sql, args)
        self.con.commit()

        return "Success"

    def get_data(self, query, param=None, is_dict=True):
        if param is None:
            param = []
        try:
            cur = self.con.cursor(dictionary=is_dict)
            cur.execute(query, param)

            data = cur.fetchall()

            self.con.commit()

            return data
        except Exception as e:
            self.logger.error(
                "An error has occurred while fetching data from db.")
            self.logger.error(f"Error: {e}")
            return e
