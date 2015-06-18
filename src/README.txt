this is one data synchor tool base on sqlalchemy.
simple start:
    python -m dsync.synchor <soucre_connstr> <target_connstr>
    data connstr is base on sqlalchemy
    exp:
    python -m dsync.synchor mysql://user:password@host:port/sourcedb mysql://user:password@host:port/targetdb
