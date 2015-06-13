# -*- coding:utf8 -*-

from builder import Builder
import sqlalchemy
try:
    import simplejson as json
except:
    import json

class Generator(object):
    @classmethod
    def get_tables(cls,conn):
        tables=conn.table_names()
        return map(unicode.upper,tables)

    @classmethod
    def get_columns(cls,conn,table,meta=None):
        meta = meta or sqlalchemy.MetaData(conn)
        t=sqlalchemy.Table(table,meta,autoload=True)
        col_list=list(t.columns)
        cols=[]
        keys=[]
        for item in col_list:
            obj=[None,None]
            obj[0]=item.name.upper()
            item_type=item.type
            if isinstance(item_type,sqlalchemy.types.Integer) \
                or isinstance(item_type,sqlalchemy.types.Numeric):
                obj[1]="num"
            elif isinstance(item_type,sqlalchemy.types.DateTime) \
                or isinstance(item_type,sqlalchemy.types.Date):
                obj[1]="date"
            else:
                obj[1]="str"
            cols.append(obj)
            if item.primary_key:
                keys.append(obj)
        cols.sort(key=lambda a:a[0])
        keys.sort(key=lambda a:a[0])
        return cols,keys

    @classmethod
    def match_one_table(cls,sconn,stable,tconn,ttable,smeta=None,tmeta=None):
        scollist,skeylist=cls.get_columns(sconn,stable,meta=smeta)
        tcollist,tkeylist=cls.get_columns(tconn,ttable,meta=tmeta)
        col_match=cls._match_fields(scollist,tcollist)
        key_match=cls._match_fields(skeylist,tkeylist)
        return {
            "col_source_remain":col_match["source_remain"],
            "col_match":col_match["match"],
            "col_target_remain":col_match["target_remain"],
            "key_source_remain":key_match["source_remain"],
            "key_match":key_match["match"],
            "key_target_remain":key_match["target_remain"],
        }
        
    @classmethod
    def _match_fields(cls,sflist,tflist):
        tdict={}
        for item in tflist:
            tdict[item[0]]=item
        slist=[]
        mlist=[]
        for sitem in sflist:
            titem=tdict.get(sitem[0],None)
            if titem:
                mlist.append([sitem,titem])
                tdict.pop(sitem[0])
            else:
                slist.append(sitem)
        tlist=tdict.values()
        tlist.sort(key=lambda a:a[0])
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
            tdict[item]=item
        slist=[]
        mlist=[]
        for sitem in stlist:
            if tdict.has_key(sitem):
                mlist.append(sitem)
                tdict.pop(sitem)
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
        for table in match_tables["match"]:
            group.append(cls.gene_one_table_config(sconn,table,tconn,table,smeta=smeta,tmeta=tmeta))
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
        from builder import Builder
        conninfo={}
        conninfo.update(connconfig)
        conninfo["charset"]=None
        conn=Builder.build_conn(conninfo)[0]
        return conn


    @classmethod
    def gene_config_to_file(cls,sconnconfig,tconnconfig,filename):
        config=cls.gene_config(sconnconfig,tconnconfig)
        with open(filename,'w') as f:
            json.dump(config,f,indent=2)





