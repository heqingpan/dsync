# -*- coding:utf8 -*-
from typeinfo import TypeInfo
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy



class Core(object):
    commit_count=500
    @classmethod
    def sync_table(cls,sconfig,tconfig):
        sconn = sconfig["conn"]
        stable = sconfig["table"]
        skeys = sconfig["keys"]
        swhere = sconfig["where"]
        sfields = sconfig["fields"]

        tconn = tconfig["conn"]
        ttable = tconfig["table"]
        tkeys = tconfig["keys"]
        twhere = tconfig["where"]
        tfields = tconfig["fields"]

        sqlfmt="select {0} from {1} where 1=1 {2} order by {3}"
        # get source data
        ssql = sqlfmt.format(cls._get_multi_fields(sfields)
                ,stable.get_name()
                ,swhere or ""
                ,cls._get_multi_fields(skeys))
        sdata=sconn.execute(ssql).fetchall()
        # get target data
        tsql = sqlfmt.format(cls._get_multi_fields(tfields)
                ,ttable.get_name()
                ,twhere or ""
                ,cls._get_multi_fields(tkeys))
        tdata=tconn.execute(tsql).fetchall()
        # after order

        si=0
        ti=0
        sl=len(sdata)
        tl=len(tdata)
        kl=len(skeys)
        fl=len(sfields)
        update_sql=None
        insert_sql=None
        delete_sql=None
        update_params_list=[]
        insert_params_list=[]
        delete_params_list=[]
        c_count=cls.commit_count
        update_index=0
        insert_index=0
        delete_index=0
        update_count=0
        insert_count=0
        delete_count=0
        equal_count=0
        while si < sl and ti < tl:
            srow = sdata[si]
            trow = tdata[ti]
            # 比较建是否相同
            key_compare=0
            for i in xrange(kl):
                skey=skeys[i]
                tkey=tkeys[i]
                svalue=srow[skey.name]
                tvalue=trow[tkey.name]
                key_compare = TypeInfo.compare_value(svalue,skey.get_type()
                        ,tvalue,tkey.get_type())
                if key_compare != 0:
                    break
            if key_compare==0:
                # 确认是否需要更新
                can_update=False
                for i in xrange(fl):
                    skey=sfields[i]
                    tkey=tfields[i]
                    svalue=srow[skey.name]
                    tvalue=trow[tkey.name]
                    compare_value = TypeInfo.compare_value(svalue,skey.get_type()
                            ,tvalue,tkey.get_type())
                    if compare_value != 0:
                        can_update=True
                        break
                if can_update:
                    # 更新
                    update_sql,params = cls._take_update(sconfig,tconfig
                            ,srow,trow,update_sql=update_sql)
                    update_params_list.append(params)
                    update_index+=1
                    update_count+=1
                    if update_index >= c_count:
                        update_index-=c_count
                        tconn.execute(update_sql,update_params_list)
                        update_params_list=[]
                else:
                    equal_count+=1
                si+=1
                ti+=1
            elif key_compare < 0:
                # 插入
                insert_sql,params = cls._take_insert(sconfig,tconfig
                        ,srow,trow,insert_sql=insert_sql)
                insert_params_list.append(params)
                insert_index+=1
                insert_count+=1
                if insert_index >= c_count:
                    insert_index-=c_count
                    tconn.execute(insert_sql,insert_params_list)
                    insert_params_list=[]
                si+=1
            else:
                # 删除
                delete_sql,params = cls._take_delete(sconfig,tconfig
                        ,srow,trow,delete_sql=delete_sql)
                delete_params_list.append(params)
                delete_index+=1
                delete_count+=1
                if delete_index >= c_count:
                    delete_index-=c_count
                    tconn.execute(delete_sql,delete_params_list)
                    delete_params_list=[]
                ti+=1
        while si < sl:
            srow = sdata[si]
            trow=None
            # 插入
            insert_sql,params = cls._take_insert(sconfig,tconfig
                    ,srow,trow,insert_sql=insert_sql)
            insert_params_list.append(params)
            insert_index+=1
            insert_count+=1
            if insert_index >= c_count:
                insert_index-=c_count
                tconn.execute(insert_sql,insert_params_list)
                insert_params_list=[]
            si+=1
        while ti < tl:
            srow=None
            trow = tdata[ti]
            # 删除
            delete_sql,params = cls._take_delete(sconfig,tconfig
                    ,srow,trow,delete_sql=delete_sql)
            delete_params_list.append(params)
            delete_index+=1
            delete_count+=1
            if delete_index >= c_count:
                delete_index-=c_count
                tconn.execute(delete_sql,delete_params_list)
                delete_params_list=[]
            ti+=1
        if update_params_list:
            tconn.execute(update_sql,update_params_list)
        if insert_params_list:
            tconn.execute(insert_sql,insert_params_list)
        if delete_params_list:
            tconn.execute(delete_sql,delete_params_list)
        return {
            "source_count":len(sdata),
            "target_count":len(tdata),
            "delete_count":delete_count,
            "update_count":update_count,
            "insert_count":insert_count,
            "equal_count":equal_count,
            }

            
    @classmethod
    def _take_update(cls,sconfig,tconfig,srow,trow,update_sql=None):
        sconn = sconfig["conn"]
        stable = sconfig["table"]
        skeys = sconfig["keys"]
        sfields = sconfig["fields"]

        tconn = tconfig["conn"]
        ttable = tconfig["table"]
        tkeys = tconfig["keys"]
        tfields = tconfig["fields"]
        kl=len(skeys)
        fl=len(sfields)

        update_sqlfmt="update {0} set {1} where {2}"
        update_sql=update_sql
        setlist=[]
        wherelist=[]
        params={}
        for i in xrange(fl):
            skey=sfields[i]
            tkey=tfields[i]
            svalue=srow[skey.name]
            middle_value=TypeInfo.get_value(svalue,skey.get_type(),tkey.get_type())
            pkey="pf{0}".format(i)
            params[pkey]=middle_value
            if type(update_sql) == type(None):
                set_one=" {0} = :{1} ".format(tkey.get_name(),pkey)
                setlist.append(set_one)
        for i in xrange(kl):
            tkey=tkeys[i]
            tvalue=trow[tkey.name]
            pkey="fk{0}".format(i)
            params[pkey]=tvalue
            if type(update_sql) == type(None):
                one_where=" {0} = :{1} ".format(tkey.get_name(),pkey)
                wherelist.append(one_where)
        if type(update_sql) == type(None):
            setstr=",".join(setlist)
            wherestr=" and ".join(wherelist)
            update_sql = update_sqlfmt.format(ttable.get_name()
                    ,setstr,wherestr)
            update_sql = sqlalchemy.text(update_sql)
        return update_sql,params

    @classmethod
    def _take_insert(cls,sconfig,tconfig,srow,trow,insert_sql=None):
        sconn = sconfig["conn"]
        stable = sconfig["table"]
        skeys = sconfig["keys"]
        sfields = sconfig["fields"]

        tconn = tconfig["conn"]
        ttable = tconfig["table"]
        tkeys = tconfig["keys"]
        tfields = tconfig["fields"]
        kl=len(skeys)
        fl=len(sfields)

        insert_sqlfmt="insert into {0} ({1}) values ({2})"
        insert_sql=insert_sql
        fieldlist=[]
        vlist=[]
        params={}
        for i in xrange(fl):
            skey=sfields[i]
            tkey=tfields[i]
            svalue=srow[skey.name]
            middle_value=TypeInfo.get_value(svalue,skey.get_type(),tkey.get_type())
            pkey="pf{0}".format(i)
            params[pkey]=middle_value
            if type(insert_sql) == type(None):
                fieldlist.append(tkey.get_name())
                vlist.append(pkey)
        if type(insert_sql) == type(None):
            namestr=",".join(fieldlist)
            valuestr=",".join(map(lambda a:":%s"%a,vlist))
            insert_sql = insert_sqlfmt.format(ttable.get_name()
                    ,namestr,valuestr)
            insert_sql = sqlalchemy.text(insert_sql)
        return insert_sql,params

    @classmethod
    def _take_delete(cls,sconfig,tconfig,srow,trow,delete_sql=None):
        sconn = sconfig["conn"]
        stable = sconfig["table"]
        skeys = sconfig["keys"]
        sfields = sconfig["fields"]

        tconn = tconfig["conn"]
        ttable = tconfig["table"]
        tkeys = tconfig["keys"]
        tfields = tconfig["fields"]
        kl=len(skeys)
        fl=len(sfields)

        delete_sqlfmt="delete from {0} where {1}"
        delete_sql=delete_sql
        wherelist=[]
        params={}
        for i in xrange(kl):
            tkey=tkeys[i]
            tvalue=trow[tkey.name]
            pkey="fk{0}".format(i)
            params[pkey]=tvalue
            if type(delete_sql) == type(None):
                one_where=" {0} = :{1} ".format(tkey.get_name(),pkey)
                wherelist.append(one_where)
        if type(delete_sql) == type(None):
            wherestr=" and ".join(wherelist)
            delete_sql = delete_sqlfmt.format(ttable.get_name()
                    ,wherestr)
            delete_sql = sqlalchemy.text(delete_sql)
        return delete_sql,params


    @classmethod
    def _get_multi_fields(cls,fields):
        return ",".join(map(lambda f:f.get_name(),fields))

