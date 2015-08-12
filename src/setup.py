try:
    from setuptools import setup
except:
    from distutils.core import setup
 
NAME = "dsync"
PACKAGES = ["dsync",]
DESCRIPTION = "this is one data synchor tool"
LONG_DESCRIPTION = """this is one data synchor tool base on sqlalchemy.
simple start:
    python -m dsync.synchor source_connstring target_connstring
    data connstr is base on sqlalchemy
    exp:
    python -m dsync.synchor mysql://user:password@host:port/sourcedb mysql://user:password@host:port/targetdb

python -m dsync -t sync source_connstring target_connstring
python -m dsync -t sync config_file
python -m dsync -t diff source_connstring target_connstring
python -m dsync -t gene source_connstring target_connstring out_file
"""
KEYWORDS = "data synchor tool,support multi database like mysql sqlite"
AUTHOR = "heqingpan"
AUTHOR_EMAIL = "heqingpan@126.com"
URL = "https://github.com/heqingpan/dsync"
VERSION = "0.2.2"
LICENSE = "MIT"
 
setup(
    name = NAME,
    version = VERSION,
    description = DESCRIPTION,
    long_description = LONG_DESCRIPTION,
    classifiers = [
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
    ],
    keywords = KEYWORDS,
    author = AUTHOR,
    author_email = AUTHOR_EMAIL,
    url = URL,
    license = LICENSE,
    packages = PACKAGES,
    include_package_data=True,
    zip_safe=True,
)
