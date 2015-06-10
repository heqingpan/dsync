# -*- coding:utf8 -*-
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy

class TypeInfo(object):
    def __init__(self,name,ftype="str",convert=str):
        self.name=name
        self.dname=convert(name)
        self.ftype=ftype
    def get_name(self):
        return self.dname
    def get_type(self):
        return self.ftype

class Common(object):
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
        print "---- 1 ----"
        print ssql
        sdata=sconn.execute(ssql).fetchall()
        print len(sdata)
        # get target data
        tsql = sqlfmt.format(cls._get_multi_fields(tfields)
                ,ttable.get_name()
                ,twhere or ""
                ,cls._get_multi_fields(tkeys))
        print "---- 2 ----"
        print tsql
        tdata=tconn.execute(tsql).fetchall()
        print len(tdata)
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
        c_count=500
        update_index=0
        insert_index=0
        delete_index=0
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
                key_compare = cls._compare_value(svalue,skey.get_type()
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
                    compare_value = cls._compare_value(svalue,skey.get_type()
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
                    if update_index >= c_count:
                        update_index-=c_count
                        tconn.execute(update_sql,update_params_list)
                        update_params_list=[]
                si+=1
                ti+=1
            elif key_compare < 0:
                # 插入
                insert_sql,params = cls._take_insert(sconfig,tconfig
                        ,srow,trow,insert_sql=insert_sql)
                insert_params_list.append(params)
                insert_index+=1
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
                if delete_index >= c_count:
                    delete_index-=c_count
                    sconn.execute(delete_sql,delete_params_list)
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
            middle_value=cls._get_value(svalue,skey.get_type(),tkey.get_type())
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
        print "----- update ------"
        print update_sql
        print params
        # update
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
            middle_value=cls._get_value(svalue,skey.get_type(),tkey.get_type())
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
        print "----- insert ------"
        print insert_sql
        print params
        # insert
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
        print "----- delete ------"
        print delete_sql
        print params
        # delete
        return delete_sql,params


    @classmethod
    def _get_multi_fields(cls,fields):
        return ",".join(map(lambda f:f.get_name(),fields))

    @classmethod
    def _compare_value(cls,svalue,stype,tvalue,ttype,get_value=None):
        sv,tv=cls._get_middle_value(svalue,stype,tvalue,ttype,get_value=get_value)
        if sv==tv:
            return 0
        if sv < tv:
            return -1
        if sv > tv:
            return 1

    @classmethod
    def _get_middle_value(cls,svalue,stype,tvalue,ttype,get_value=None):
        base_type=cls._get_base_type(stype,ttype)
        get_value= get_value or cls._get_value
        return (get_value(svalue,stype,base_type)
                ,get_value(tvalue,ttype,base_type))

    @classmethod
    def _get_base_type2(cls,stype,ttype):
        base_type=None
        if stype==ttype:
            base_type=stype
        elif stype=="str" and ttype=="num" or \
            ttype=="num" and stype=="str":
            base_type="num"
        elif stype=="num" and ttype=="date" or \
            ttype=="date" and stype=="num":
            base_type="date"
        elif stype=="str" or ttype=="str":
            base_type="str"
        return base_type

    @classmethod
    def _get_base_type(cls,stype,ttype):
        return ttype


    @classmethod
    def _get_value(cls,value,form_type,to_type):
        if value is None or form_type==to_type:
            return value
        r=value
        if to_type=="str" and form_type=="date":
            try:
                r=value.strftime("%Y-%m-%d %H:%M:%S")
            except:
                pass
        elif to_type=="str":
            r=str(value)
        elif to_type=="num" and form_type=="date":
            r=(value-datetime.datetime.fromtimestamp(0)).total_seconds()
        elif to_type=="num":
            r=float(value)
        elif to_type=="date" and form_type=="num":
            r=datetime.datetime.fromtimestamp(value)
        elif to_type=="date" and form_type=="str":
            r=datetime.datetime.strptime(value,"%Y-%m-%d %H:%M:%S")
        return r

    @classmethod
    def build_test_00(cls):
        sconnstr=r"mysql+mysqldb://admin:admin@127.0.0.1:3306/bigagio?charset=utf8"
        tconnstr=r"mysql+mysqldb://admin:admin@127.0.0.1:3306/test01?charset=utf8"
        sconn= create_engine(sconnstr)
        tconn= create_engine(tconnstr)
        #sengine = create_engine(sconnstr)
        #tengine = create_engine(tconnstr)
        #s_Session=sessionmaker()
        #s_Session.configure(bind=sengine)
        #sconn=s_Session()
        #t_Session=sessionmaker()
        #t_Session.configure(bind=tengine)
        #tconn=t_Session()

        stable=ttable=TypeInfo("TB_SYSCONFIG")
        skeys=tkeys=[TypeInfo("ID","str")]
        swhere=twhere=None
        sfields=tfields=[
                TypeInfo("ID","str"),
                TypeInfo("SYSKEY","str"),
                TypeInfo("NAME","str"),
                TypeInfo("VALUE","str"),
            ]

        sconfig={
            "conn":sconn,
            "table":stable,
            "keys":skeys,
            "fields":sfields,
            "where":swhere,
            }
        tconfig={
            "conn":tconn,
            "table":ttable,
            "keys":tkeys,
            "fields":tfields,
            "where":twhere,
            }
        return sconfig,tconfig

    @classmethod
    def test_00(cls):
        sconfig,tconfig = cls.build_test_00()
        cls.sync_table(sconfig,tconfig)
        

