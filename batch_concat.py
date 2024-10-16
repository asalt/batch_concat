"""CLI for finding and concatenating
all output files in a directory from Proteome Discoverer
2.0, and then optionally moving the output file into
in a different directory"""
import sys
import os
from pathlib import Path
import re
import shutil
from collections import defaultdict
from configparser import ConfigParser
import click
import pandas as pd
from tqdm import tqdm # nice double progress bar
from utils import *

__version__ = '0.9.3'
__author__  = 'Alexander Saltzman'

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

class Config(object):

    def __init__(self):
        self.groups = None

class FileGroup(object):

    def __init__(self, files=None, searchno=None):
        self._name = None
        self._files = files
        self._recno = None
        self._runno = None
        self.searchno = searchno
        self.past_record = False
        self.updating = False
        self.passed = True
        self._added_search = False

    def __str__(self):
        return self.name

    def __repr__(self):
        return 'File group {}'.format(self.name)

    def __len__(self):
        return len(self.files)
    def __iter__(self):
        return iter(self.files)
    @property
    def files(self):
        return self._files

    @files.setter
    def files(self, files):
        self._name = None
        self._recno = None
        self._runno = None
        self._files = files

    def filter_files(self, indices):
        """Pop files by index"""
        self._files = [x for x in self._files if self._files.index(x) in indices]
        return self

    @property
    def recno(self):
        if self.files and not self._recno:
            self.set_rec_run()
        return int(self._recno)

    @property
    def runno(self):
        if self.files and not self._runno:
            self.set_rec_run()
        return int(self._runno)

    @property
    def name(self):
        if (self._name and
            self.searchno and not
            re.search(r'^\d{{5}}_\d+_{}_'.format(self.searchno), self._name)):
            self.insert_search()
        if self.files and not self._name:
            self.set_name()
            if self.searchno:
                self.insert_search()
        return self._name

    def insert_search(self):
        if self._added_search:
            return
        pat = re.compile(r'^\d{3,5}_\d+_')
        try:
            self._name = pat.sub('{}{}_'.format(pat.search(self._name).group(), self.searchno), self._name)
            self._added_search = True
        except AttributeError:
            raise 'Could not assign search'

    def set_name(self):
        if len(self.files) < 1:
            raise AttributeError('No files in this group')
        name = ''
        for ix, char in enumerate(self.files[0].name):
            if all(self.files[x].name[ix] == char for x in range(len(self))):
                name += char
            else:
                name += 'all.txt'
                break
        else:
            name += '_all.txt'
        self._name = name

    def set_rec_run(self):
        pat_recno = re.compile(r'^\d{3,5}')
        pat_runno = re.compile(r'(?<=\d_)(\d+)(?=_)')
        recno = pat_recno.search(self.name)
        runno = pat_runno.search(self.name)
        if recno:
            self._recno = recno.group()
        if runno:
            self._runno = runno.group()

pass_config = click.make_pass_decorator(Config, ensure=True)

def batch_concat(filegroups, outputdir=None, stout=None):
    for filegroup in filegroups:
        display(filegroup)
    if not click.confirm('Would you like to proceed'):
        click.echo('Exiting..', file=stout)
        sys.exit(0)
    for filegroup in tqdm(filegroups, desc='Total groups'):
        data = list()
        for file in tqdm(filegroup, desc=filegroup.name):
            df = pd.read_table(file.absolute())
            data.append(filter_output(df))
            df = pd.concat(data)
            df.to_csv(os.path.join(outputdir, filegroup.name), index=False, sep='\t')

        if filegroup.updating:
            update_recrun(filegroup.recno, filegroup.runno, filegroup.searchno)
            delete_concat(filegroup.recno, filegroup.runno, filegroup.searchno)
        else:
            insert_new_run(recno=filegroup.recno,
                                    runno=filegroup.runno,
                                    searchno=filegroup.searchno)
        insert_new_concat(filegroup)

def stage_batch_concat(filegroup, inputdir=None, outputdir=None):

    if outputdir is None:
        outputdir = '.'
    updating = False
    if previous_concat(filegroup.recno, filegroup.runno, filegroup.searchno):
        if not click.confirm(("{}_{}_{} has previously been concatenated."
        "Are you sure you wish to proceed?").format(filegroup.recno,
                                                    filegroup.runno,
                                                    filegroup.searchno)):
            filegroup.passed = False
            return
        else:
            filegroup.updating = True
            select_files(filegroup)
            return

    if filegroup.past_record:
        if not click.confirm(("{}_{} has previously been concatenated. "
       "Are you sure you wish to proceed?").format(filegroup.recno,
                                                    filegroup.runno)):
            filegroup.passed = False
            return
        select_files(filegroup)

def assign_searches(filegroups):

    for filegroup in filegroups:
        filegroup.searchno = click.prompt('Enter searchno for {}_{}'.format(filegroup.recno, filegroup.runno),
                                          type=int)
    return filegroups

def file_grouper(groups, force=False, path=None, runno=None):

    filegroups = list()
    for files in groups.values():
        filegroup = FileGroup(files)
        if runno:
            # print(filegroup.runno)
            if filegroup.runno != runno: continue
        filegroup.past_record = previous_concat(filegroup.recno, filegroup.runno, path=path)
        if (not filegroup.past_record or force):
            filegroups.append(filegroup)
    return filegroups

def file_checker(inputdir=None, outputdir=None, target_str='TargetPeptideSpectrumMatch|psms', ignore=None,
                 exclusive_groups=None, force=False, stout=None):
    """Gets groups of files"""
    if inputdir is None:
        inputdir = '.'
    if outputdir is None:
        outputdir = '.'
    if ignore is None:
        ignore = tuple()

    pat = re.compile(r'^\d{3,5}_\d+_')
    psms_re = re.compile(target_str, re.I)
    run_re = re.compile(r'^\d+')
    groups = defaultdict(list)
    #for entry in os.scandir(inputdir):
    for entry in Path(inputdir).rglob("*"):
        # if entry.is_file() and target_str in entry.name:
        if entry.is_file() and psms_re.search(entry.name):
            group = pat.search(entry.name)
            if group:
                g = group.group()
                g_run = int(run_re.search(g).group())
                if any(x==g_run for x in ignore):
                    g = None
                if exclusive_groups:
                    if g_run not in exclusive_groups:
                        g = None
                if g:
                    groups[g].append(entry)
            else:
                click.echo('Improper file name : {}'.format(entry.name), file=stout)
    return groups

@click.group(invoke_without_command=True, context_settings=CONTEXT_SETTINGS)
@click.pass_context
#@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('-i', '--ignore', multiple=True, type=int,
              help='List of experiment groups to ignore for this session,'\
              ' all other possible groups will be included.')
@click.option('-f', '--force', is_flag=True,
              help='Force the concatenation of experiments even if they have been concatenated before.')
@click.option('-g', '--groups', multiple=True, type=int,
              help='List of experiment groups to include,'\
              ' all other possible groups will be ignored for this session.')
@click.option('-p', '--preview', is_flag=True,
              help='View groups of files that would be concatenated'\
              ' without actually performing the concatenation and exit.')
@click.option('-s', '--source', type=click.Path(exists=True, file_okay=False),
              help='Set the source directory.')
@click.option('-t', '--target', type=click.Path(exists=True, file_okay=False),
              help='Set the target directory.')
@click.option('-l', '--log', type=click.File('w'), default='-',)
@click.option('-r', '--runno', type=int,
              help='''Constrain to a certain run number.
              Good for use in conjunction with --groups flag.''')
def cli(ctx, ignore, force, groups, preview, source, target, log, runno):

    if source:
        source = os.path.abspath(source)
    if target:
        target = os.path.abspath(target)

    if ctx.invoked_subcommand is None:
        click.echo('Running normal batch concat', file=log)
        if (set(ignore) & set(groups)):
            click.secho('Overlap between list of experiments to ignore and group', fg='red')
            raise click.Abort
        directories = get_directories()  # get source and target directories
        if directories.get('source') is None and source is None:
            directories['source'] = click.prompt('Enter source directory', default='.', type=click.Path(exists=True, file_okay=False),
                                                 value_proc=os.path.abspath)
        if directories.get('target') is None and target is None:
            directories['target'] = click.prompt('Enter target directory', default='.', type=click.Path(exists=True, file_okay=False),
                                                 value_proc=os.path.abspath)
        if source and source != directories.get('source'):
            update_directory(source, 'source')
        if target and target != directories.get('target'):
            update_directory(target, 'target')
        fgroups = file_checker(directories.get('source'), directories.get('target'), stout=log,
                               exclusive_groups=groups, ignore=ignore)

        filegroups = file_grouper(fgroups, force=force, runno=runno)
        if len(filegroups) == 0:
            click.echo('No files to group!', file=log)
            sys.exit(0)

        click.echo('\n{} filegroup(s) found'.format(len(filegroups)))
        for filegroup in filegroups:
            display(filegroup)

        if preview or not click.confirm('Would you like to continue?'):
            click.echo('Exiting without concatenating anything', file=log)
            return

        filegroups = assign_searches(filegroups)

        for filegroup in filegroups:
            stage_batch_concat(filegroup)
        filegroups = [filegroup for filegroup in filegroups if filegroup.passed]

        if len(filegroups) == 0:
            click.echo('No files to group!', file=log)
            sys.exit(0)
        batch_concat(filegroups, outputdir=directories.get('target'))
    else:
        pass
        # click.echo('Running a special function', file=log)

@cli.command()
@click.argument('recnos', nargs=-1)
def remove(recnos):
    for recno in recnos:
        runno = click.prompt('Enter the runno for {}'.format(recno), default=1)
        searchno = click.prompt('Enter the searchno for {}'.format(recno), default=1)
        if click.confirm('Are you sure you wish to delete {}_{}_{}'.format(recno, runno, searchno)):
            delete_concat(recno, runno, searchno)
            delete_recrun(recno, runno, searchno)
            click.echo('Removing an entry')

@cli.command()
@click.argument('recnos', nargs=-1)
def add(recnos):
    """Provide a list of record numbers to be manually added to the log."""
    for recno in recnos:
        runno = click.prompt('Enter the runno for {}'.format(recno), default=1)
        searchno = click.prompt('Enter the searchno for {}'.format(recno), default=1)
        click.secho('Adding {}_{}_{}'.format(recno, runno, searchno), fg='green')
        insert_new_run(recno, runno, searchno)
