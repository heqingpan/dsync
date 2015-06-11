# -*- coding:utf8 -*-

from typeinfo import TypeInfo
import sqlalchemy

class Builder(object):
    @classmethod
    def build(cls,config):
        sconnconfig=config["sconn"]
        sinfo = cls.build_conn(sconnconfig)
        tconnconfig=config["tconn"]
        tinfo = cls.build_conn(tconnconfig)
        group = config["group"]
        return cls.build_group(sinfo,tinfo,group)

    @classmethod
    def build_group(cls,sinfo,tinfo,group):
        rlist=[]
        for element in group:
            rlist.append(cls.build_element(sinfo,tinfo,element))
        return rlist

    @classmethod
    def build_element(cls,sinfo,tinfo,element):
        sconn=sinfo[0]
        sconvert=sinfo[1]
        tconn=tinfo[0]
        tconvert=tinfo[1]

        s,t=element[0],element[1]
        sconfig=cls._build_element_one(sconn,sconvert,s)
        tconfig=cls._build_element_one(tconn,tconvert,t)
        return sconfig,tconfig
        

    @classmethod
    def _build_element_one(cls,conn,convert,info):
        config={}
        config["conn"]=conn
        config["table"]=TypeInfo(info["table"],convert=convert)
        keys=[]
        for key in info["keys"]:
            keys.append(TypeInfo(key[0],key[1]
                ,convert=convert))
        config["keys"]=keys
        fields=[]
        for field in info["fields"]:
            fields.append(TypeInfo(field[0],field[1]
                ,convert=convert))
        config["fields"]=fields
        config["where"]=info["where"]
        return config

    @classmethod
    def build_conn(cls,config):
        return cls.build_conn_info(config.get("type",None),**config)

    @classmethod
    def build_conn_info(cls,database_type,**kwarg):
        if database_type=="mysql":
            return cls.mysql_conn_info(**kwarg)
        connstr=kwarg and kwarg.get("connstr",None)
        if database_type=="sqlite":
            return cls.sqlite_conn_info(**kwarg)
        if connstr:
            return sqlalchemy.create_engine(connstr),str
        return cls.default_conn_info(**kwarg)

    @classmethod
    def default_conn_info(cls,**kwarg):
        return mysql_conn_info(**kwarg)[0],str

    @classmethod
    def mysql_conn_info(cls,**kwarg):
        host=kwarg.get("host","127.0.0.1")
        port=kwarg.get("port","3306")
        dbname=kwarg.get("dbname","test")
        user=kwarg.get("user","root")
        password=kwarg.get("password","")
        charset=kwarg.get("charset","utf8")
        engine_type=kwarg.get("engine_type","mysql+mysqldb")
        connstr="{0}://{1}:{2}@{3}:{4}/{5}".format(engine_type
                ,user,password
                ,host,port
                ,dbname)
        if charset:
            connstr = connstr + "?charset=" + charset
        return sqlalchemy.create_engine(connstr),(lambda a:'`%s`'%a)

    @classmethod
    def sqlite_conn_info(cls,**kwarg):
        connstr="sqlite:///:memory:"
        path=kwarg.get("path",None)
        if path:
            path = os.path.realpath(path)
            connstr="sqlite://"+path
        return sqlalchemy.create_engine(connstr),str 


