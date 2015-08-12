# data synchro
a data synchor tool

this is one data synchor tool base on sqlalchemy.
simple start:
    python -m dsync.synchor source_connstring target_connstring
    data connstr is base on sqlalchemy
    exp:
    python -m dsync.synchor mysql://user:password@host:port/sourcedb mysql://user:password@host:port/targetdb

python -m dsync -t sync source_connstring target_connstring
python -m dsync -t sync config_file
python -m dsync -t diff source_connstring target_connstring
python -m dsync -t gene source_connstring target_connstring out_file

