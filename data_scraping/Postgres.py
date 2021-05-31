import psycopg2
import sys
from typing import Dict, Union


class Postgres():

    def __init__(self, host: str, database: str, user: str, password: str) -> None:

        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.conn = None

        self._connect()


    def does_table_exist(self, table: str) -> bool:

            command = "SELECT * FROM pg_catalog.pg_tables"
            rows = self.command(command, 'r')

            return table.lower() in [row[1].lower() for row in rows]


    def command(self, command: str, mode: str, str_params: Dict[str, str]={}) -> Union[None, str]:

        if mode not in ('r', 'w'): # read and write
            print("ERROR: mode parameter must be 'r' or 'w'")
            sys.exit()

        cur = self.conn.cursor()

        try:
            cur.execute(command, str_params)
        except Exception as e:
            print(e)
            sys.exit()
        
        if mode == 'r':
            rows = cur.fetchall()
            cur.close()
            
            return rows

        else:
            cur.close()
            self.conn.commit()


    def _connect(self) -> None:

        try:
            conn = psycopg2.connect(
                host = self.host,
                database = self.database,
                user = self.user,
                password = self.password
            )

            self.conn = conn

        except Exception as e:
            print(e)
            sys.exit()