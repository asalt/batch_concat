from setuptools import setup

def calculate_version(inputfile):
    version_list =  [x.split('\'')[1] for x in open(inputfile, 'r')
                     if x.startswith('__version__')]
    if version_list:
        return version_list[0]
    else:
        return '1.0'

package_version = calculate_version('batch_concat.py')
setup(
    name='BatchConcat',
    version=package_version,
    py_modules=['batch_concat', 'utils', 'test'],
    install_requires=[
        'Click',
    ],
    entry_points="""
    [console_scripts]
    batch_concat=batch_concat:cli
    """
)
