import os
import string
from io import StringIO
import unittest
from unittest import mock
from datetime import date, datetime
from click.testing import CliRunner
from batch_concat import *
from utils import *
from utils import __db__, __config__

thedate = datetime.strptime('1990/09/20 11:11:11', '%Y/%m/%d %H:%M:%S')

class my_stat_result(object):
    def __init__(self):
        self.st_atime = datetime.timestamp(thedate)
        self.st_ctime = datetime.timestamp(thedate)
        self.st_mtime = datetime.timestamp(thedate)
        self.st_size = 1000
    def __repr__(self):
        return """myos.my_stat_result(st_atime={}, st_ctime={}
        st_mtime={}, st_size={})""".format(self.st_atime,
                                           self.st_ctime,
                                           self.st_mtime,
                                           self.st_size)

    def __str__(self):
        return """myos.my_stat_result(st_atime={}, st_ctime={}
        st_mtime={}, st_size={})""".format(self.st_atime,
                                           self.st_ctime,
                                           self.st_mtime,
                                           self.st_size)

class MyDirEntry(object):

    def __init__(self, name=None, pathname=None):
        self.name = name
        self._pathname = pathname
        self._stat = my_stat_result
    def __repr__(self):
        return "<DirEntry '{}'>".format(self.name)
    def __str__(self):
        return "<DirEntry '{}'>".format(self.name)
    def is_file(self):
        return True
    def is_dir(self):
        return False
    def is_symlink(self):
        return False
    @property
    def path(self):
        if self._pathname is None:
            return os.path.join('.', self.name)
        return os.path.join(self._pathname, self.name)
    @property
    def stat(self):
        return self._stat

def make_fake_files(recno=12345, runno=1, n=5, pathname=None):
    files = list()
    for c in string.ascii_lowercase[:n]:
        files.append(MyDirEntry('{}_{}_TargetPeptideSpectrumMatch_{}'.format(recno,
                                                                            runno,
                                                                            c), pathname))
    return files


stout = StringIO()  # capture all of the click.echos here

class SqliteTest(unittest.TestCase):
    def setUp(self):
        if os.path.exists(__db__):
            os.remove(__db__)

    def test_setup(self):
        make_database('.', stout=stout)
        insert_new_run(12345, 1, 1, path='.')
        filegroup = FileGroup(make_fake_files(), 1)
        insert_new_concat(filegroup, path='.')
        conn = get_connection(path='.')
        c = conn.cursor()
        c.execute("""SELECT 1 from concat_files
        WHERE rec_run=?""", (1,))
        fetch = c.fetchall()
        self.assertEqual(len(fetch), 5)
        # TODO assert proper insertions into database
        # But no errors means database is created successfully

    def teaDown(self):
        os.remove(__db__)

class BatchTest(unittest.TestCase):
    def setUp(self):
        """Create a database and put some fake data into it"""
        # print('Making new database')
        if os.path.exists(__db__):
            os.remove(__db__)
        make_database('.', stout=stout)
        insert_new_run(12345, 1, 1, path='.')
        filegroup = FileGroup(make_fake_files(), 1)
        insert_new_concat(filegroup, path='.')

    def tearDown(self):
        """Remove database"""
        # print('Cleaning up..')
        os.remove(__db__)

    def test_previous_concat(self):
        self.assertTrue(previous_concat(12345,1,1, path='.'))

    def test_update_recrun(self):
        update_recrun(12345,1,1, path='.')
        conn = get_connection(path='.')
        c = conn.cursor()
        c.execute("""SELECT modification_ts from exprun
        WHERE recno=? AND runno=? AND searchno=?""", (12345,1,1))
        fetch = c.fetchall()
        self.assertEqual(len(fetch),1)
        logged_dt = fetch[0][0]
        self.assertEqual(logged_dt.date(), date.today())

    def test_delete_concat(self):

        delete_concat(12345,1,1, path='.')
        conn = get_connection(path='.')
        c = conn.cursor()
        c.execute("""SELECT 1 from concat_files
        WHERE rec_run=?""", (1,))
        fetch = c.fetchall()
        self.assertEqual(len(fetch), 0)

    def test_groups(self):
        with mock.patch('os.scandir') as scandir:
            fake_entry = make_fake_files(12346)
            scandir.return_value = fake_entry
            groups = file_checker(stout=stout)
            for entry, char, in zip(groups['12346_1_'], string.ascii_lowercase):
                self.assertEqual(entry.name,
                                 '12346_1_TargetPeptideSpectrumMatch_{}'.format(char))

    def test_file_grouper(self):
        groups = {'12346_1_': make_fake_files(12346)}
        filegroups = file_grouper(groups, path='.')

    @mock.patch('click.confirm')
    @mock.patch('click.prompt')
    def test_select_files(self, mock_prompt, mock_confirm):
        mock_prompt.return_value = (0, 1, 3)
        mock_confirm.return_value = True
        filegroup = FileGroup(make_fake_files(), 1)
        select_files(filegroup, stout=stout)
        # runner = CliRunner()
        # runner.invoke(select_files(filegroup, stout=stout), input='0 1 2')
        for file, char in zip(filegroup.files, ('a', 'b', 'd')):
                self.assertEqual(file.name,
                                 '12345_1_TargetPeptideSpectrumMatch_{}'.format(char))

    def test_specify_runno(self):
        """Test if we can specify a single file group"""
        groups = {'12347_1_': make_fake_files(12347, 1),
                  '12347_2_': make_fake_files(12347, 2),
        }
        # print(groups.values()
        filegroups = file_grouper(groups, runno=2, path='.')

        self.assertEqual(len(filegroups), 1)
        self.assertEqual(filegroups[0].runno, 2)
    # def test_cli_new(self):
        # def
        # runner = CliRunner()
        # runner.invoke(select_files(filegroup, stout=stout), input='0 1 2')
if __name__ == '__main__':
    unittest.main()