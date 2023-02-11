#!/usr/bin/python3

"""
Das Python-Skript erstellt Informationen zu den Dateien.
Die Pruefung erfolgt ueber das Kommando md5sum.
"""

import os
import sys, getopt      # Argumente parsen
import logging

import sqlite3
import hashlib
import datetime

global home_path
global db_name

home_path = os.environ['HOME']
db_name = "Database/FileInfos.sqlite"


def usage (proc_name, cash_count, datenbank, mount_point, current_dir, storage):
    print(
        """
            Syntax:

            ${PROC} -h
            ${PROC} [Optionen]

            Optionen:
              -h                       Dieser Hilfetext.
              -c <Cash_count>          Anzahl der Datensätze, welche zwischen gespeichert werden,
                                       bevor die Daten geschrieben werden - default ist {CASH_COUNT}.
              -d <Datenbank>           Datenbank - default is '{DATENBANK}'
              -m <Mount-Point>         Pfad des Einhängepunktes - default ist '{MOUNT_POINT}'.
              -q <Quellverzeichnis>    Quellverzeichnis - default ist '{CURRENTDIR}'.
              -s <Festplatte>          Storage - default ist '{STORAGE}'.
        """.format(PROC=proc_name, CASH_COUNT=cash_count, DATENBANK=datenbank, MOUNT_POINT=mount_point, CURRENTDIR=current_dir, STORAGE=storage))


class DataList:
    def __init__(self, con, cash_count):
        self.con = con
        self.cash_count = cash_count
        self.data_insert = []         # data_insert   = tuple(md5sum, groesse, mtime, file_name, file_path, storage)
        self.data_upd_md5 = []        # data_upd_md5  = tuple(md5sum, groesse, mtime, file_name, file_path, storage)
        self.data_upd_time = []       # data_upd_time = tuple(file_name, file_path, storage)
        self.data_get = []            # data_get      = tuple(file_name, md5sum, groesse, mtime)


    def _update_md5sum (self):
        try:
            sql = """
                UPDATE tblDateien
                    SET   last_update = strftime('%Y-%m-%d %H-%M-%S','now'), md5sum = ?, groesse = ?, mtime = ?, flag = 'udp'
                    WHERE file_name = ?
                    AND   file_path = ?
                    AND   storage = ?;"""

            cursor = self.con.cursor()
            cursor.executemany(sql, self.data_upd_md5)
            self.con.commit()
            logging.info("Total {} Records updated md5sum successfully into database table.".format(cursor.rowcount))
            cursor.close()

        except sqlite3.Error as error:
            logging.error("Failed to update multiple records into database table. Error: %s", error)


    def update_md5sum (self, md5sum, groesse, mtime, file_name, file_path, storage):
        file_name = file_name.replace("'", "_")
        file_path = file_path.replace("'", "_")
        
        data_tuple = (md5sum, groesse, mtime, file_name, file_path, storage)
        self.data_upd_md5.append(data_tuple)
        if len(self.data_upd_md5) >= self.cash_count:
            self._update_md5sum()
            self.data_upd_md5 = []


    def _update_time (self):
        try:
            sql = """
                UPDATE tblDateien
                    SET last_update = strftime('%Y-%m-%d %H-%M-%S', 'now')
                    WHERE file_name = ?
                    AND   file_path = ?
                    AND   storage = ?;"""

            cursor = self.con.cursor()
            cursor.executemany(sql, self.data_upd_time)
            self.con.commit()
            logging.info("Total {} Records updated time successfully into database table.".format(cursor.rowcount))
            cursor.close()

        except sqlite3.Error as error:
            logging.error("Failed to update multiple records into database table. Error: %s", error)


    def update_time (self, file_name, file_path, storage):
        file_name = file_name.replace("'", "_")
        file_path = file_path.replace("'", "_")

        data_tuple = (file_name, file_path, storage)
        self.data_upd_time.append(data_tuple)
        if len(self.data_upd_time) >= self.cash_count:
            self._update_time()
            self.data_upd_time = []


    def _insert (self):
        try:
            sql = """
                INSERT INTO tblDateien
                    (md5sum, groesse, mtime, file_name, file_path, storage, last_update, flag)
                VALUES
                    (?, ?, ?, ?, ?, ?, strftime('%Y-%m-%d %H-%M-%S','now'),'neu');
                """
            
            cursor = self.con.cursor()
            cursor.executemany(sql, self.data_insert)
            self.con.commit()
            logging.info("Total {} Records inserted successfully into database table.".format(cursor.rowcount))
            cursor.close()
            
        except sqlite3.Error as error:
            logging.error("Failed to insert multiple records into sqlite table. Error: %s", error)


    def insert (self, md5sum, groesse, mtime, file_name, file_path, storage):
        # SQL --> ' im Namen
        file_name = file_name.replace("'", "_")
        file_path = file_path.replace("'", "_")
        
        data_tuple = (md5sum, groesse, mtime, file_name, file_path, storage)
        self.data_insert.append(data_tuple)
        
        if len(self.data_insert) >= self.cash_count:
            self._insert()
            self.data_insert = []
        

    def _get_md5sum (self, file_path, storage):
        self.data_get = []
        try:
            sql = """
                SELECT file_name, md5sum, groesse, mtime
                    FROM tblDateien
                    WHERE file_path = ?
                    AND   storage = ?;"""

            data_tuple = (file_path, storage)

            cursor_select = self.con.cursor()
            cursor_select.execute(sql, data_tuple)
            rows = cursor_select.fetchall()

            for row in rows:
                self.data_get.append(row)

        except sqlite3.Error as error:
            logging.error("Failed to get records from database table. Error: %s", error)


    def get_md5sum (self, file_name, file_path, storage):
        db_md5sum = None
        db_groesse = None
        db_mtime = None
        file_name = file_name.replace("'", "_")
        file_path = file_path.replace("'", "_")
        
        if len(self.data_get) == 0:
            self._get_md5sum (file_path, storage)
        else:    
            if file_path != self.data_get[0][1]:
                self._get_md5sum (file_path, storage)
        
        for data_tuple in self.data_get:
            if data_tuple[0] == file_name:
                db_md5sum  = data_tuple[1]
                db_groesse = data_tuple[2]
                db_mtime   = data_tuple[3]
                self.data_get.remove(data_tuple)
                return db_md5sum, db_groesse, db_mtime
                
        return db_md5sum, db_groesse, db_mtime


    def flash (self):
        if len(self.data_insert) > 0:
            self._insert()
            self.data_insert = []
        
        if len(self.data_upd_md5) > 0:
            self._update_md5sum()
            self.data_upd_md5 = []

        if len(self.data_upd_time) > 0:
            self._update_time()
            self.data_upd_time = []



def md5sum(filename, blocksize=65536):
    hash = hashlib.md5()
    with open(filename, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            hash.update(block)
    return hash.hexdigest()


def dir_search (depth, data_list, mount_point, source_dir, storage):
    db_md5sum = ''
    db_groesse = 0
    db_mtime = ''
    
    os.chdir(source_dir)
    current_dir = os.getcwd()
    db_dir = current_dir.replace(mount_point, ".", 1)
    logging.info("-- {0} - CurPath = '{1}'".format(depth, current_dir))
    
    list_dir = os.listdir(current_dir)

    if depth <= 4:
        print("-- {} - {} - CurPath = '{}'".format(depth, len(list_dir), current_dir)) 
    
    logging.info("-- {0} - DB_Path = '{1}'".format(depth, db_dir))
    
    for entry in list_dir:
        if not os.path.islink(entry):
            if os.path.isfile(entry):
                logging.info("   {0} -> File: '{1}'".format(depth, entry))

                # Datei größe
                # file_groesse = os.path.getsize( os.path.join( '.', entry ) )
                file_groesse = os.path.getsize( entry )
                
                # file modification time
                # file_mtime = os.path.getmtime( entry )
                file_mtime = datetime.datetime.fromtimestamp(os.path.getmtime( entry )).strftime('%Y-%m-%d %H:%M:%S')
                
                # MD5SUM, Groesse, und M-Time aus der Datenbank holen
                db_md5sum, db_groesse, db_mtime = data_list.get_md5sum(entry, db_dir, storage)
                
                logging.debug("        DB_MD5SUM = '{}'".format(db_md5sum))
                logging.debug("        DB-Größe   = {}, MTime = '{}'".format(db_groesse, db_mtime))
                logging.debug("        File-Größe = {}, mtime = '{}'".format(file_groesse, file_mtime))

                if db_md5sum == None:
                    # Informationen einfügen

                    # md5sum von der Datei
                    file_md5sum = md5sum(entry)
                
                    logging.debug ("Insert")
                    data_list.insert(file_md5sum, file_groesse, file_mtime, entry, db_dir, storage)
                
                else:
                    if file_groesse != db_groesse or file_mtime != db_mtime:
                        # md5sum von der Datei
                        file_md5sum = md5sum(entry)

                        # Update md5sum und flag
                        logging.debug ("update_md5sum")
                        data_list.update_md5sum(file_md5sum, file_groesse, file_mtime, entry, db_dir, storage)

                    else:                    
                        # Update last_update
                        logging.debug ("update_time")
                        data_list.update_time(entry, db_dir, storage)
            
            else:
                if not entry == "System Volume Information":
                    logging.debug("   {0} -> Dir : '{1}'".format(depth, entry))
                    data_list = dir_search((depth + 1), data_list, mount_point, entry, storage)
                    os.chdir(current_dir)
    
    logging.debug("<- {0} - CPath = '{1}'".format(depth, source_dir))
    return data_list
    

def main(argv):
    global home_path
    
    # Initialisierung
    logfilename = home_path + "/SaveFileInfos.log"
    logging.basicConfig(filename=logfilename,format='%(asctime)s - %(levelname)s - %(message)s',level=logging.INFO)
    
    # Default-Werte
    datenbank = "file:" + home_path + "/" + db_name + "?mode=rw"
    source_dir="."
    storage="root"
    mount_point=""
    cash_count=100
    
    try:
        opts, args = getopt.getopt(argv,"hc:d:m:q:s:",["cash_count=", "datenbank=", "mount_point=", "source_dir=", "storage="])
    except getopt.GetoptError:
        usage(str(sys.argv[0]), cash_count, datenbank, mount_point, source_dir, storage)
        sys.exit(1)

    for opt, arg in opts:
        if opt == '-h':
            usage(str(sys.argv[0]), cash_count, datenbank, mount_point, source_dir, storage)
            sys.exit(0)
        elif opt in ("-c", "--cash_count"):
            cash_count = int(arg)
        elif opt in ("-d", "--datenbank"):
            datenbank = arg
        elif opt in ("-m", "--mount_point"):
            mount_point = arg
        elif opt in ("-q", "--source_dir"):
            source_dir = arg
        elif opt in ("-s", "--storage"):
            storage = arg
    
    try:
        #datenbank = db_name + "?mode=ro"
        datenbank = "file:" + home_path + "/" + db_name + "?mode=rw"
        
        logging.info("=============================================================================")
        logging.info("Parameter:")
        logging.info("Datenbank       : '%s'", datenbank)
        logging.info("Mount-Point     : '%s'", mount_point)
        logging.info("Quellverzeichnis: '%s'", source_dir)
        logging.info("Storage         : '%s'", storage)
        logging.info("cash_count      :  %d ", cash_count)

        print ("Start : ", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        logging.info("Connecting the database")
        sqliteConnection = sqlite3.connect(datenbank, uri=True)
        
        # Objekt erstellen -> new
        data_list = DataList(sqliteConnection, cash_count)

        # Suche und Abgleich starten
        data_list = dir_search(1, data_list, mount_point, source_dir, storage)
        
        data_list.flash()

        if sqliteConnection:
            sqliteConnection.close()
            print('SQLite Connection closed')

    except sqlite3.Error as err:
         print('SQLite error: %s' % (' '.join(err.args)))

    print ("Ende  : ", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
 
if __name__ == "__main__": 
    main(sys.argv[1:])

