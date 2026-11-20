import sqlite3
import json
import threading

from pathlib import Path

class Database:
    """
    Encapsulate Database features
    """

    def __init__(self, RESET: bool = False):
        """
        Create the database when object initiated

        :param RESET: Delete the database before recreating it
        """

        # the sqlite3 connection to a database
        self.connection = None

        # Keep track of all connections so they can be closed later
        self._local = threading.local()
        self._connections = set()
        self._connections_lock = threading.Lock()

        self.database_path = Path(__file__).resolve().parent / "database.db"

        self.sql_script_path = Path(__file__).resolve().parent / "create_table.sql"

        if RESET:
            self.__delete_db()
        self.__init_db()


    """ ==============  TEARDOWN  =============="""

    def close(self):
        """
        Close connection to database
        """

        with self._connections_lock:

            for db in self._connections:
                db.close()

            self._connections.clear()


    """ ==============  INSERT  ============== """

    def insert_file(self, filename: str, size: int, content_type: str, storage_details: dict, version: int) -> int | None:
        """
        Wrapper for inserting a file into the database table for files

        :param str filename: The name of the file
        :param int size: The size of the file in bytes
        :param str content_type: A string representing the type of file
        :param dict storage_details: A dictionary with the neccecary information to retreive the file from nodes
        :param int version: The file version

        :return id: The id of the created file entry
        """
        
        db = self.__get_db()

        storage_details_json = json.dumps(storage_details)

        cursor = db.execute(
            "INSERT INTO `file`(`filename`, `size`, `content_type`, `storage_details`, `version`) VALUES (?,?,?,?,?)",
            (filename, size, content_type, storage_details_json, version)
        )
        db.commit()

        return cursor.lastrowid


    def insert_hash(self, hash: str) -> int | None:
        """
        A wrapper for inserting a hash block into the database hash table

        :param str hash: The hashed value of data block from a file

        :return id: The id of the inserted hash block  
        """
        db = self.__get_db()

        existing = self.select_existing_hash([hash])
        if existing:
            return existing[0]["id"]

        cursor = db.execute(
            "INSERT INTO block_hash (hash) VALUES (?)",
            (hash,)
        )

        db.commit()

        return cursor.lastrowid


    """ ==============  SELECT  ============== """

    def select_all_files(self) -> list[dict] | None:
        """
        A wrapper for fetching all items from the database file table 

        :return files: A list of files with all table entries warpped in a dictionary
        """

        db = self.__get_db()
        cursor = db.execute("SELECT * FROM `file`")
        
        files = cursor.fetchall()

        if not files:
            return None

        return self.__parse_file_entries(files)


    def select_one_file(self, filename: str) -> list[dict] | None:
        """
        A wrapper for fetching all copies of a single file

        :param int file_id: The id of the desired file

        :return file: A file with all table entries warpped in a dictionary
        """


        db = self.__get_db()
        cursor = db.execute("SELECT * FROM `file` WHERE `filename`=?", [filename])
    
        files = cursor.fetchall()


        if files is None:
            return None

        return self.__parse_file_entries(files)


    def select_existing_hash(self, hashes: list[str]) -> list[dict] | None:
        """
        A wrapper for fetching the hashes which exist in the given list of hashes

        :param hashes: A list of hashes for file 

        :return existing_hashes: A list of dictionaries with all hashes already existing in the database
        """

        
        db = self.__get_db()
        query = "SELECT * FROM `block_hash` WHERE `hash` IN ({placeholders})".format(
            placeholders=','.join(['?'] * len(hashes))
        )
        cursor = db.execute(query, hashes)
        existing_hashes = cursor.fetchall()
        existing_hashes = [dict(h) for h in existing_hashes]

        if not existing_hashes:
            return None

        return existing_hashes


    """ ==============  DB UTILITY  ============== """

    def __get_db(self) -> sqlite3.Connection:
        """
        Ensure the database connection is set and ensure only 1 thread can use the connection

        :return connection: The Connection to the database
        """

        # Get the connection belonging to the current thread
        db = getattr(self._local, "connection", None)

        if db is None:
            db = sqlite3.connect(
                self.database_path,
                detect_types=sqlite3.PARSE_DECLTYPES,
                timeout=10.0
            )

            db.row_factory = sqlite3.Row

            # Save connection for this thread
            self._local.connection = db

            # Register connection for cleanup
            with self._connections_lock:
                self._connections.add(db)

        return db


    def __init_db(self):
        """
        Initialise the database with the sql script
        """

        if self.database_path.exists():
            return

        db = self.__get_db()

        if self.sql_script_path.exists():
            with open(self.sql_script_path) as sql_file:

                # split each statement, then drop empty spaces between statements
                sql_statements = [script for script in sql_file.read().split(";") if len(script) > 1]
                if not sql_statements:
                    return False

                for statement in sql_statements:
                    db.execute(statement)
                    db.commit()

    
    def __delete_db(self):
        if self.database_path.exists():
            self.database_path.unlink()
            
    def __parse_file_entries(self, file_entry: list) -> list[dict]:
        """
        Parse the file table entries

        :param list file_entry: The fetched entries in the `file` table form a query

        :return result: The result as a dictionary 
        """

        files = [dict(row) for row in file_entry]

        for file in files:
            file["storage_details"] = json.loads(
                file["storage_details"]
            )

        return files
    
if __name__ == "__main__":
    db = Database(RESET=False)
    print(db.select_all_files())

        
    