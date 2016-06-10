#!/usr/bin/python
# -*- coding: utf-8 -*-

from PySide import QtCore, QtGui
import os
import sys

import sqlite3
import datetime

import logging

"""
"""

FOLDERPATH_WHITESPACE_STRIP = True

def folder_path_normalize(folder):
    '''directory separators normalized (converted to "/") and 
    redundant ones removed, and "."s and ".."s resolved (as far as possible).
    Symbolic links are kept.  '''
    path = QtCore.QDir.cleanPath(folder)
    if FOLDERPATH_WHITESPACE_STRIP == True:
        path = path.strip()
    ''' with the '/' separators converted to separators that are appropriate 
    for the underlying operating system.'''
    return QtCore.QDir.toNativeSeparators(path)
    # there is also path = dir.canonicalPath()
    # but this would check existence, too
    
def test_folder_path_normalize():
    print("BOL"+folder_path_normalize(r"C:\test\test\\// ")+"EOL")

# both target folderpath and file extension are normalized in this module!
# extension '(none)' is generated in parent module
# data_folders_flat, data_categories_tree therefore get clean references!

DATABASE_FILE_EXTENSION_IS_LOWERCASE = True
DATABASE_VERSION_MINIMUM = 3
DATABASE_VERSION_CURRENT = 3


class Model:
    def __init__(self, database_filepath="loadstar_sqlite3.db"):
        """ Is called once in 
        """
        self.database_filepath = database_filepath
        
        sqlite3.register_adapter(bool, int)
        sqlite3.register_converter("BOOLEAN", lambda v: bool(int(v)))

        
        self.connect_args = {'database':self.database_filepath,
                             'detect_types':sqlite3.PARSE_DECLTYPES}

        self.initial_run = not os.path.exists(database_filepath)
        
        if self.initial_run:
            logging.info('DB: Creating schema')
            with sqlite3.connect(**self.connect_args) as conn:
                """ Alternative:
                cursor.execute('''CREATE TABLE IF NOT EXISTS
                    users(id INTEGER PRIMARY KEY, name TEXT, phone TEXT, email TEXT unique, password TEXT)''')
                """
                schema = """
                CREATE TABLE `target_folder` (
                    `folder_path`	TEXT,
                    `alive_checks_failed`	INTEGER,
                    `flag_bookmark`	BOOLEAN,
                    `flag_explorer_open`	BOOLEAN,
                    `flag_private`	BOOLEAN,
                    `flag_retired`	BOOLEAN,
                    PRIMARY KEY(folder_path)
                );
                CREATE TABLE `move_latest` (
                    `filename_length`	INTEGER,
                    `file_extension`	TEXT,
                    `target_folder`	TEXT,
                    `moved_latest_date`	timestamp,
                    `moved_times`	INTEGER,
                    PRIMARY KEY(filename_length,file_extension)
                );
                """
                conn.executescript(schema)
            
        else:
            logging.info('DB: file exists, assume schema does, too.')
            self.folders_postload_exist_check()
         
            #logging.debug("Folders post-load normalization and sanitization:")
            #data_folders_flat.folders_postload_normalize_sanitize(self.folders_flat_dict)

        # TODO check version with pragma user_version
        
 
        
    def folders_postload_exist_check(self):
        """if bookmark: recheck always, unless retired
        if other: recheck 5 times then retire location
        if reactivated by any means, remove retirement flag
        """

        def dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d
    
        with sqlite3.connect(**self.connect_args) as conn:
            conn.row_factory = dict_factory
            cur = conn.cursor()
            rows = cur.execute("""SELECT folder_path, alive_checks_failed, 
                        flag_bookmark, flag_retired FROM target_folder""").fetchall()

        with sqlite3.connect(**self.connect_args) as conn:
            
            for row in rows:
                ff = row['folder_path']
                if row['flag_retired'] == False and \
                            not QtCore.QDir(ff).exists():
                    ffe = ff.encode('ascii', 'backslashreplace')
                    logging.debug("DB: Folder not found, failing alive check: %s" % (ffe,))
                
                    update_alive = row['alive_checks_failed'] + 1
                    conn.execute("""UPDATE OR IGNORE target_folder 
                        SET alive_checks_failed = ?
                        WHERE folder_path = ?""", (update_alive, ff))
                    
                    if update_alive > 4 and row['flag_bookmark'] != True:
                        conn.execute("""UPDATE OR IGNORE target_folder 
                            SET flag_retired = ?
                            WHERE folder_path = ?""", (True, ff))
                        logging.info("DB: Folder temporarily retired: %s" % (ffe,))
    
                        
    def bookmark_add(self, folderpath):
        ff = folder_path_normalize(folderpath)
        
        
        # also possible: INSERT OR IGNORE
        
        with sqlite3.connect(**self.connect_args) as conn:
            cur = conn.cursor()
            row = cur.execute("SELECT * FROM target_folder WHERE folder_path=?", 
                    (ff, )).fetchone()
            if row:
                conn.execute("""UPDATE target_folder 
                    SET flag_bookmark = ?
                    WHERE folder_path = ?""", (True, ff))
                result = "already_in_data"
            else:
                conn.execute("""INSERT INTO target_folder 
                    (folder_path, alive_checks_failed, flag_bookmark, 
                     flag_explorer_open, flag_private, flag_retired)
                    VALUES(?, ?, ?, ?, ?, ?)""", 
                    (ff, 0, True, False, False, False))
                result = "added"
        return result

        
    def bookmark_remove(self, folderpath):
        ff = folder_path_normalize(folderpath)
        
        with sqlite3.connect(**self.connect_args) as conn:
            conn.execute("""UPDATE OR IGNORE target_folder 
                SET flag_bookmark = ?
                WHERE folder_path = ?""", (False, ff))
        
        
    def folder_remove(self, folderpath):
        ff = folder_path_normalize(folderpath)
        ffe = ff.encode('ascii', 'backslashreplace')
        
        # It's possible to do this also with foreign keys and ON DELETE CASCADE
        # but then I would have to call a pragma before each connection.
        # https://www.sqlite.org/foreignkeys.html
        
        with sqlite3.connect(**self.connect_args) as conn:
            cur = conn.cursor()
            row = cur.execute("SELECT * FROM target_folder WHERE folder_path=?", 
                    (ff, )).fetchone()
            if row:
                conn.execute("""DELETE FROM target_folder 
                    WHERE folder_path = ?""", (ff,))
                logging.debug('SQL: Removed from target_folder: %s' % (ffe,))
                
            row = cur.execute("SELECT * FROM move_latest WHERE target_folder=?", 
                    (ff, )).fetchone()
            if row:
                conn.execute("""DELETE FROM move_latest 
                    WHERE target_folder = ?""", (ff,))
                logging.debug('SQL: Removed from move_latest: %s' % (ffe,))
            
        
    def folder_flag_set(self, folderpath, flag_name, flag_bool):
        ff = folder_path_normalize(folderpath)
        
        with sqlite3.connect(**self.connect_args) as conn:
            conn.execute("""UPDATE OR IGNORE target_folder 
                SET """ + flag_name + """=?
                WHERE folder_path = ?""", (flag_bool, ff))
    
    def folder_flag_get(self, folderpath, flag_name):
        ff = folder_path_normalize(folderpath)
        
        with sqlite3.connect(**self.connect_args) as conn:
            row = conn.execute("""SELECT """ + flag_name + 
                    """ FROM target_folder WHERE folder_path=?""", 
                               (ff, )).fetchone()
            return bool(row[0])
        
        
    def file_explorer_toggle(self, folderpath):
        if self.file_explorer_get(folderpath):
            self.file_explorer_set(folderpath, False)
        else:
            self.file_explorer_set(folderpath, True)
                    
    def file_explorer_set(self, folderpath, open_bool):
        self.folder_flag_set(folderpath, "flag_explorer_open", open_bool)
     
    def file_explorer_get(self, folderpath):
        return self.folder_flag_get(folderpath, "flag_explorer_open")
                
    def private_set(self, folderpath, private_bool):
        self.folder_flag_set(folderpath, "flag_private", private_bool)
                
    def private_get(self, folderpath):
        return self.folder_flag_get(folderpath, "flag_private")
           
                
    def statistics_update_post_move(self, folderpath, filename):
        ff = folder_path_normalize(folderpath)
        
        # add fresh folder in case we use alternative move to methods
        with sqlite3.connect(**self.connect_args) as conn:
            conn.execute("""INSERT OR IGNORE INTO target_folder 
                    (folder_path, alive_checks_failed, flag_bookmark, 
                     flag_explorer_open, flag_private, flag_retired)
                    VALUES(?, ?, ?, ?, ?, ?)""", 
                    (ff, 0, False, False, False, False))
    
        file_basename, file_extension = os.path.splitext(filename)
        filename_length = len(file_basename)
        if file_extension == "":
            file_extension_db = "(none)"
        else:
            file_extension_db = file_extension.lower()
        now = datetime.datetime.now()
        
        with sqlite3.connect(**self.connect_args) as conn:
            conn.execute("""INSERT OR IGNORE INTO move_latest 
                (file_extension, filename_length, target_folder, 
                 moved_latest_date, moved_times)
                VALUES(?, ?, ?, ?, ?)""", 
                (file_extension_db, filename_length, ff, now, 0))
            conn.execute("""UPDATE move_latest 
                SET moved_latest_date=? , moved_times=moved_times+1
                WHERE target_folder=? and file_extension=? and filename_length=?""", 
                (now, ff, file_extension_db, filename_length))
             
            
class View():
    def __init__(self, model):
        self.model = model
            
    def str_from_boolean(self, bool):
        return 'yes' if bool else 'no'

    def bookmarks_generate(self, private_include=False):
        with sqlite3.connect(**self.model.connect_args) as conn:
            # Find all bookmarked folders
            rows = conn.execute("""SELECT folder_path, flag_explorer_open 
                    FROM target_folder WHERE flag_bookmark=? AND 
                    flag_retired=? AND flag_private IN (?,?)""",(True, False, 
                                        False, private_include)).fetchall()

            for row in rows:
                # For each bookmarked folder, get additional data
                moved_times = conn.execute("""SELECT SUM(moved_times) 
                    FROM move_latest WHERE target_folder=?""",(row[0],)).fetchone()[0]
                if not moved_times:
                    moved_times = 0
                timestamp_str = conn.execute("""SELECT MAX(moved_latest_date) 
                    FROM move_latest WHERE target_folder=?""",(row[0],)).fetchone()[0]
                if timestamp_str:
                    timestamp = datetime.datetime.strptime(
                            timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
                    moved_latest_date_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    moved_latest_date_str = ''
                
                yield(str(moved_times), row[0], moved_latest_date_str, 
                      self.str_from_boolean(row[1]))

                
    def all_generate(self, private_include=False):
        with sqlite3.connect(**self.model.connect_args) as conn:
            # Find all folders
            rows = conn.execute("""SELECT folder_path, flag_explorer_open 
                    FROM target_folder WHERE  
                    flag_retired=? AND flag_private IN (?,?)""",(False, 
                                        False, private_include)).fetchall()
            for row in rows:
                # For each folder, get additional data
                moved_times = conn.execute("""SELECT SUM(moved_times) 
                    FROM move_latest WHERE target_folder=?""",(row[0],)).fetchone()[0]
                if not moved_times:
                    moved_times = 0
                timestamp_str = conn.execute("""SELECT MAX(moved_latest_date) 
                    FROM move_latest WHERE target_folder=?""",(row[0],)).fetchone()[0]
                if timestamp_str:
                    timestamp = datetime.datetime.strptime(
                            timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
                    moved_latest_date_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    moved_latest_date_str = ''
                
                yield(str(moved_times), row[0], moved_latest_date_str, 
                      self.str_from_boolean(row[1]))
                      
                      
    def by_extension_generate(self, file_extension, private_include=False):
        file_extension_db = file_extension
        if DATABASE_FILE_EXTENSION_IS_LOWERCASE:
            file_extension_db = file_extension.lower()
        with sqlite3.connect(**self.model.connect_args) as conn:
            # Find all valid folders that file_extensions were moved to
            rows = conn.execute("""SELECT folder_path, flag_explorer_open 
                    FROM target_folder WHERE  
                    flag_retired=? AND flag_private IN (?,?) AND
                    folder_path IN (SELECT DISTINCT target_folder 
                                    FROM move_latest WHERE  
                                    file_extension=?)""",
                    (False, False, private_include, file_extension_db)).fetchall()
            for row in rows:
                # For each folder, get additional data
                moved_times = conn.execute("""SELECT SUM(moved_times) 
                    FROM move_latest WHERE target_folder=? AND 
                    file_extension=?""",(row[0],file_extension_db)).fetchone()[0]
                if not moved_times:
                    moved_times = 0
                timestamp_str = conn.execute("""SELECT MAX(moved_latest_date) 
                    FROM move_latest WHERE target_folder=? AND
                    file_extension=?""",(row[0],file_extension_db)).fetchone()[0]
                if timestamp_str:
                    timestamp = datetime.datetime.strptime(
                            timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
                    moved_latest_date_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    moved_latest_date_str = ''
                
                yield(str(moved_times), row[0], moved_latest_date_str, 
                      self.str_from_boolean(row[1]))
    
       
    def by_extension_and_length_generate(self, file_extension, fn_len, private_include=False):
        file_extension_db = file_extension
        if DATABASE_FILE_EXTENSION_IS_LOWERCASE:
            file_extension_db = file_extension.lower()
            
        with sqlite3.connect(**self.model.connect_args) as conn:
            # Find all valid folders that file_extensions were moved to
            rows = conn.execute("""SELECT folder_path, flag_explorer_open 
                    FROM target_folder WHERE  
                    flag_retired=? AND flag_private IN (?,?) AND
                    folder_path IN (SELECT DISTINCT target_folder 
                                    FROM move_latest WHERE  
                                    file_extension=? AND filename_length=?)""",
                    (False, False, private_include, file_extension_db, fn_len)).fetchall()
            #print rows, fn_len
            for row in rows:
                # For each folder, get additional data
                moved_times = conn.execute("""SELECT SUM(moved_times) 
                    FROM move_latest WHERE target_folder=? AND 
                    file_extension=? AND filename_length=?""",(row[0],file_extension_db,fn_len)).fetchone()[0]
                if not moved_times:
                    moved_times = 0
                timestamp_str = conn.execute("""SELECT MAX(moved_latest_date) 
                    FROM move_latest WHERE target_folder=? AND
                    file_extension=? AND filename_length=?""",(row[0],file_extension_db,fn_len)).fetchone()[0]
                if timestamp_str:
                    timestamp = datetime.datetime.strptime(
                            timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
                    moved_latest_date_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    moved_latest_date_str = ''
                
                yield(str(moved_times), row[0], moved_latest_date_str, 
                      self.str_from_boolean(row[1]))    
    
   
def test_bookmark_add():
    app = QtGui.QApplication(sys.argv)
    folder_selected = QtGui.QFileDialog.getExistingDirectory(
            caption="Select your Downloads folder!",dir=QtCore.QDir.homePath())
    
    model = Model()
    
    model.bookmark_add(folder_selected)
    
    app.quit()
    print("Adding bookmark: DONE")
    
def test_view_bookmarks():
    model = Model()
    view = View(model)
    
    filename = 'test.pdf'
    _, file_extension = os.path.splitext(filename)

    fn_len = len(os.path.basename(filename))
    fn_len = 14
        
    print("----------------------------------------------------------------")
    print("Generating view with bookmarks:")
    for ll in view.all_generate(False):
         #ffe = ll[0].encode('ascii', 'backslashreplace')
        print ll
    print("Generating view with bookmarks: DONE")
    
def test_by_extension_generate():
    model = Model()
    view = View(model)
    
    filename = 'test.tt'
    _, file_extension = os.path.splitext(filename)

    fn_len = len(os.path.basename(filename))
    fn_len = 14
        
    print("----------------------------------------------------------------")
    print("test_by_extension_generate")
    for ll in view.by_extension_generate(file_extension, False):
        ffe = ll[0].encode('ascii', 'backslashreplace')
        print ll

   
   
def test_view_generate_class():
    model = Model()
    view = View(model)
    
    filename = 'test.txt'
    _, file_extension = os.path.splitext(filename)

    fn_len = len(os.path.basename(filename))
    print os.path.basename(filename)
    print fn_len
    fn_len = 4
        
    print("----------------------------------------------------------------")
    print("Generating view with bookmarks:")
    for ll in view.bookmarks_generate():
         #ffe = ll[0].encode('ascii', 'backslashreplace')
        print ll
    print("Generating view with bookmarks: DONE")
    
    print("----------------------------------------------------------------")
    print("Generating view with extension:")
    for ll in view.by_extension_generate(file_extension):
        print ll
    print("Generating view with extension: DONE")
    
    print("----------------------------------------------------------------")
    print("Generating view with extension/filename length:")
    for ll in view.by_extension_and_length_generate(file_extension, fn_len):
        print ll
    print("Generating view with extension/filename length: DONE")
    
    
    print("----------------------------------------------------------------")
    print("Generating view with all")
    for ll in view.all_generate():
        print ll
    print("Generating view with all: DONE")
    
def test_model():
    model = Model()
    print model.bookmark_add("test")
    #print model.bookmark_add("test")
    print model.bookmark_add("test2")
    #model.bookmark_remove("test")
    #model.folder_remove("test")
    model.file_explorer_set("test",True)
    print model.file_explorer_get("test")
    model.file_explorer_set("test",False)
    print model.file_explorer_get("test")
    model.folders_postload_exist_check()
    model.statistics_update_post_move("test", "test.txt")
    model.statistics_update_post_move("test", "test2.txt")
    model.statistics_update_post_move("test2asdf", "test2.txt")
    model.statistics_update_post_move("test2asdf", "test2asdf")
    #model.folder_remove("test")
    

   
if __name__ == "__main__": 
    #test_bookmark_add()
    test_model()
    #test_by_extension_generate()
    test_view_generate_class()
    
    
