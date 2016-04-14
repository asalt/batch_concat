import os
import re
from datetime import datetime
import sqlite3 as sql
from configparser import ConfigParser
import click

__config__  = 'batch_concat.ini'
__db__ = 'batch_concat.sqlite'
__basedir__ = os.path.expanduser('~')
parser = ConfigParser()

def filter_output(df):
    '''Filter the file output because PD2.0 doesn't do it '''
    return df[(df['Percolator q-Value'] <= 0.05) &
              (df['Rank'] == 1)]

def make_database(path, stout=None):
    click.echo('Making a new database.', file=stout)
    conn = sql.connect(os.path.join(path, __db__), detect_types=sql.PARSE_DECLTYPES)
    #conn.execute("""CREATE TABLE experiment(
    #recno INTEGER PRIMARY KEY,
    #creation timestamp)"""
    #)
    conn.execute("""CREATE TABLE exprun(
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    recno INTEGER NOT NULL,
    runno INTEGER NOT NULL,
    searchno INTEGER NOT NULL,
    creation_ts timestamp,
    modification_ts timestamp
    )""")
    conn.execute("""CREATE TABLE concat_files(
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    rec_run INTEGER,
    filename STRING,
    filesize REAL,
    filedate DATE,
    FOREIGN KEY(rec_run) REFERENCES EXPRUN(id)
    )""")
    conn.commit()
    conn.close()
    click.secho('New database created', fg='green', file=stout)

def get_connection(path=None):
    """Return a connection to the sql database"""
    if path is None:
        path = os.path.join(__basedir__, '.batch_concat')
    db = os.path.join(path, __db__)
    if not os.path.isfile(db):
        make_database(path)
    return sql.connect(db, detect_types=sql.PARSE_DECLTYPES)

def insert_new_run(recno=None, runno=None, searchno=None, path=None):
    """Insert a new run into the database"""
    conn = get_connection(path=path)
    c = conn.cursor()
    c.execute("SELECT 1 from exprun where recno=? and runno=? and searchno=?",
              (recno, runno, searchno))
    fetch = c.fetchall()
    if len(fetch) != 0:
        click.secho('Warning: Record already exists', fg='red')
        conn.close()
        return
    c.execute("""INSERT into exprun(recno, runno, searchno, creation_ts, modification_ts)
    values (?, ?, ?, ?, ?)""", (recno, runno, searchno, datetime.now(), datetime.now()))
    conn.commit()
    conn.close()

def insert_new_concat(filestruct, path=None):
    """Insert a new file that is being batch_concatenated"""
    conn = get_connection(path=path)
    c = conn.cursor()
    c.execute("""SELECT id from exprun WHERE
    recno=? AND runno=? AND searchno=?""", (filestruct.recno, filestruct.runno, filestruct.searchno))
    idquery = c.fetchall()
    assert len(idquery) == 1
    rec_run = [x for y in idquery for x in y][0]  # all sql queries return lists of tuples
    for file in filestruct.files:
        size = file.stat().st_size
        dt = file.stat().st_mtime
        c.execute("""INSERT into concat_files(rec_run, filename, filesize, filedate)
        VALUES (?, ?, ?, ?)""", (rec_run, file.name, size, datetime.fromtimestamp(dt)))
    conn.commit()
    conn.close()

def previous_concat(recno=None, runno=None, searchno=None, path=None):
    """Returns True if previously concatenated based on
    recno, runno, and (optional) searchno"""

    conn = get_connection(path=path)
    c = conn.cursor()
    if searchno is not None:
        c.execute("SELECT 1 from exprun where recno=? and runno=? and searchno=?",
                  (recno, runno, searchno))
    else:
        c.execute("SELECT 1 from exprun where recno=? and runno=?",
                  (recno, runno))
    if len(c.fetchall()) > 0:
        return True
    else:
        return False

def update_recrun(recno=None, runno=None, searchno=None, path=None):
    """Update the exprun table with new timestamp"""
    conn = get_connection(path=path)
    c = conn.cursor()
    c.execute("""UPDATE exprun
    SET modification_ts=?
    WHERE recno=? AND runno=? AND searchno=?""", (datetime.now(), recno, runno, searchno))
    conn.commit()
    conn.close()

def delete_concat(recno=None, runno=None, searchno=None, path=None):

    conn = get_connection(path=path)
    c = conn.cursor()
    c.execute("""SELECT id from exprun WHERE
    recno=? AND runno=? AND searchno=?""", (recno, runno, searchno))
    idquery = c.fetchall()
    if len(idquery) != 1:
        click.echo('No concatenated file records to remove')
        conn.close()
        return
    rec_run = [x for y in idquery for x in y][0]  # all sql queries return lists of tuples
    c.execute("""DELETE FROM concat_files
    WHERE rec_run=?""", (rec_run,))
    conn.commit()
    conn.close()

def delete_recrun(recno=None, runno=None, searchno=None, path=None):
    conn = get_connection(path=path)
    c = conn.cursor()
    c.execute("""DELETE FROM exprun WHERE
    recno=? AND runno=? and searchno=?""", (recno, runno, searchno))
    conn.commit()
    conn.close()

def make_configfile(path=None):
    """Make a configfile with necessary sections.
    Default places it in os.path.expanduser home directory"""
    if path is None:
        path = os.path.join(__basedir__, '.batch_concat')
    if not os.path.isdir(path):
        os.mkdir(path)
    parser['directories'] = {'source': '.',
                             'target': '.'}
    with open(os.path.join(path, __config__), 'w') as configfile:
        parser.write(configfile)

def get_parser(path=None):
    """Get the parser for the configfile"""
    if path is None:
        path = os.path.join(__basedir__, '.batch_concat')
    configfile = os.path.join(path, __config__)
    if not os.path.isfile(configfile):
        make_configfile(path)
    parser.read(configfile)
    return parser

def update_directory(directory, category, path=None):

    parser = get_parser(path=path)
    parser['directories'][category] = directory
    update_config(parser)

def get_directories(path=None):

    parser = get_parser(path=path)
    return parser['directories']

def update_config(parser, path=None):

    if path is None:
        path = os.path.join(__basedir__, '.batch_concat')
    with open(os.path.join(path, __config__), 'w') as configfile:
        parser.write(configfile)

def display(filegroup, to_display=None, stout=None):
    click.echo('\nGroup : {}'.format(filegroup.name), file=stout)
    for ix, file in enumerate(filegroup.files):
        if to_display and str(ix) not in to_display:
            continue
        click.echo("({}) -- {} {} {}".format(ix,
                                             file.name,
                                             datetime.fromtimestamp(file.stat().st_mtime),
                                             byte_formatter(file.stat().st_size)), file=stout)

def select_files(filegroup, stout=None):
    """A CLI for selecting files"""
    to_display = None
    while True:
        display(filegroup, to_display, stout=stout)
        to_display = click.prompt('Select the files to use',
                                  value_proc=lambda x: re.findall(r'(?<!\w)\d+(?!\w)', x))
        display(filegroup, to_display, stout=stout)
        if click.confirm('Does this look right?'):
            break
        else: to_display = None
    filegroup.filter_files([int(x) for x in to_display])
    #click.prompt

def byte_formatter(b):
    conv = b/(2**10)
    if conv < 1000:
        return '{:.4f} kB'.format(conv)
    elif conv > 1000:
        conv = conv/(2**10)
        if conv < 1000:
            return '{:.4f} mB'.format(conv)
        elif conv < 1000:
            conv = conv/(2**10)
            return '{:.4f} GB'.format(conv)
