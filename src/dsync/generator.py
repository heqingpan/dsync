# -*- coding:utf8 -*-

from .builder import Builder
import sqlalchemy
try:
    import simplejson as json
except:
    import json

class Generator(object):
    @classmethod
    def get_tables(cls,conn):
        tables=conn.table_names()
        return map(str,tables)

    @classmethod
    def get_columns(cls,conn,table,meta=None):
        meta = meta or sqlalchemy.MetaData(conn)
        t=sqlalchemy.Table(table,meta,autoload=True)
        col_list=list(t.columns)
        cols=[]
        keys=[]
        for item in col_list:
            obj={}
            obj["name"]=item.name.upper()
            item_type=item.type
            if isinstance(item_type,sqlalchemy.types.Integer) \
                or isinstance(item_type,sqlalchemy.types.Numeric):
                obj["type"]="num"
            elif isinstance(item_type,sqlalchemy.types.DateTime) \
                or isinstance(item_type,sqlalchemy.types.Date):
                obj["type"]="date"
            else:
                obj["type"]="str"
            obj["length"]=item_type.__dict__.get("length",0) or item_type.__dict__.get("display_width",0)
            cols.append(obj)
            if item.primary_key:
                keys.append(obj)
        cols.sort(key=lambda a:a["name"])
        keys.sort(key=lambda a:a["name"])
        return cols,keys

    @classmethod
    def match_one_table(cls,sconn,stable,tconn,ttable,smeta=None,tmeta=None,check_length=0):
        scollist,skeylist=cls.get_columns(sconn,stable,meta=smeta)
        tcollist,tkeylist=cls.get_columns(tconn,ttable,meta=tmeta)
        col_match=cls._match_fields(scollist,tcollist,check_length=check_length)
        key_match=cls._match_fields(skeylist,tkeylist,check_length=check_length)
        return {
            "col_source_remain":col_match["source_remain"],
            "col_match":col_match["match"],
            "col_target_remain":col_match["target_remain"],
            "key_source_remain":key_match["source_remain"],
            "key_match":key_match["match"],
            "key_target_remain":key_match["target_remain"],
        }
        
    @classmethod
    def _match_fields(cls,sflist,tflist,check_length=0):
        tdict={}
        for item in tflist:
            tdict[item["name"]]=item
        slist=[]
        mlist=[]
        for sitem in sflist:
            titem=tdict.get(sitem["name"],None)
            if titem:
                if check_length and \
                        sitem.get("length",0) != titem.get("length",0):
                    slist.append(sitem)
                    continue
                mlist.append([sitem,titem])
                tdict.pop(sitem["name"])
            else:
                slist.append(sitem)
        tlist=tdict.values()
        tlist.sort(key=lambda a:a["name"])
        return {
            "source_remain":slist,
            "match":mlist,
            "target_remain":tlist,
        }


    @classmethod
    def gene_one_table_config(cls,sconn,stable,tconn,ttable,smeta=None,tmeta=None):
        m=cls.match_one_table(sconn,stable,tconn,ttable,smeta=smeta,tmeta=tmeta)
        skeys=[]
        tkeys=[]
        scols=[]
        tcols=[]
        for item in m["key_match"]:
            skeys.append(item[0])
            tkeys.append(item[1])
        for item in m["col_match"]:
            scols.append(item[0])
            tcols.append(item[1])
        sobj={}
        sobj["table"]=stable
        sobj["keys"]=skeys
        sobj["fields"]=scols
        sobj["where"]=""
        tobj={}
        tobj["table"]=ttable
        tobj["keys"]=tkeys
        tobj["fields"]=tcols
        tobj["where"]=""
        return sobj,tobj

    @classmethod
    def match_tables(cls,sconn,tconn):
        stlist=cls.get_tables(sconn)
        ttlist=cls.get_tables(tconn)
        tdict={}
        for item in ttlist:
            kitem=item.upper()
            tdict[kitem]=item
        slist=[]
        mlist=[]
        for sitem in stlist:
            kitem=sitem.upper()
            titem=tdict.get(kitem,None)
            if titem:
                mlist.append((sitem,titem))
                tdict.pop(kitem)
            else:
                slist.append(sitem)
        tlist=tdict.values()
        tlist.sort()
        return {
            "source_remain":slist,
            "match":mlist,
            "target_remain":tlist,
        }

    @classmethod
    def gene_group_config(cls,sconn,tconn):
        match_tables=cls.match_tables(sconn,tconn)
        smeta = sqlalchemy.MetaData(sconn)
        tmeta = sqlalchemy.MetaData(tconn)
        group = [] 
        for stable,ttable in match_tables["match"]:
            group.append(cls.gene_one_table_config(sconn,stable,tconn,ttable,smeta=smeta,tmeta=tmeta))
        return group

    @classmethod
    def gene_config(cls,sconnconfig,tconnconfig):
        sconn=cls._build_generate_conn(sconnconfig)
        tconn=cls._build_generate_conn(tconnconfig)
        group_config = cls.gene_group_config(sconn,tconn)
        return {
            "sconn":sconnconfig,
            "tconn":tconnconfig,
            "group":group_config,
        }

    @classmethod
    def _build_generate_conn(cls,connconfig):
        from .builder import Builder
        conninfo={}
        conninfo.update(connconfig)
        connstr=conninfo.get("connstr",None)
        if connstr:
            return cls._build_path_generate_conn(connstr)
        conninfo["charset"]=None
        conn=Builder.build_conn(conninfo)[0]
        return conn

    @classmethod
    def _build_path_generate_conn(cls,connstr):
        from .builder import Builder
        import urlparse
        import urllib
        t=connstr.split("?")
        if len(t)==2:
            path=t[0]
            query=t[1].lower()
            ql=urlparse.parse_qsl(query)
            rql=[]
            for item in ql:
                if item[0]!="charset":
                    rql.append(item)
            query=urllib.urlencode(rql)
            connstr="{0}?{1}".format(path,query)
        conn=Builder.path_conn_info(connstr)[0]
        return conn


    @classmethod
    def gene_config_to_file(cls,sconnconfig,tconnconfig,filename):
        config=cls.gene_config(sconnconfig,tconnconfig)
        with open(filename,'w') as f:
            json.dump(config,f,indent=2)

    @classmethod
    def get_path_connconfig(cls,path):
        return {
            "connstr":path
        }


class DiffGenerator(Generator):
    @classmethod
    def gene_diff_tables(cls,sconn,tconn,check_length=1):
        match_info = cls.match_tables(sconn,tconn)
        smeta = sqlalchemy.MetaData(sconn)
        tmeta = sqlalchemy.MetaData(tconn)
        group = []
        for stable,ttable in match_info["match"]:
            item=cls.get_table_diff_info(sconn,stable,tconn,ttable,smeta=smeta,tmeta=tmeta,check_length=check_length)
            if item:
                obj={}
                obj.update(item)
                obj["stable"]=stable
                obj["ttable"]=ttable
                group.append(obj)
        result ={}
        result["source_remain"]=match_info["source_remain"]
        result["target_remain"]=match_info["target_remain"]
        result["diff_tables"]=group
        return result

    @classmethod
    def get_table_diff_info(cls,sconn,stable,tconn,ttable,smeta=None,tmeta=None,check_length=0):
        match_info=cls.match_one_table(sconn,stable,tconn,ttable,smeta=smeta,tmeta=tmeta,check_length=check_length)
        if match_info["col_source_remain"] or \
            match_info["col_target_remain"] or \
            match_info["key_source_remain"] or \
            match_info["key_target_remain"]:
                obj={}
                obj["col_source_remain"]=match_info["col_source_remain"]
                obj["col_target_remain"]=match_info["col_target_remain"]
                obj["key_source_remain"]=match_info["key_source_remain"]
                obj["key_target_remain"]=match_info["key_target_remain"]
                return obj
        return None

    @classmethod
    def gene_diff_tables_by_path(cls,spath,tpath,check_length=1):
        sconn = cls._build_path_generate_conn(spath)
        tconn = cls._build_path_generate_conn(tpath)
        return cls.gene_diff_tables(sconn,tconn,check_length=check_length)




