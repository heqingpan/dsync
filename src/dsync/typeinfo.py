# -*- coding:utf8 -*-
import datetime


class TypeInfo(object):
    def __init__(self,name,ftype="str",convert=str):
        self.name=name
        self.dname=convert(name)
        self.ftype=ftype
    def get_name(self):
        return self.dname
    def get_type(self):
        return self.ftype

    @classmethod
    def get_base_type(cls,stype,ttype):
        return ttype

    @classmethod
    def get_value(cls,value,form_type,to_type):
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
    def get_middle_value(cls,svalue,stype,tvalue,ttype,get_value=None):
        base_type=cls.get_base_type(stype,ttype)
        get_value= get_value or cls.get_value
        return (get_value(svalue,stype,base_type)
                ,get_value(tvalue,ttype,base_type))

    @classmethod
    def compare_value(cls,svalue,stype,tvalue,ttype,get_value=None):
        sv,tv=cls.get_middle_value(svalue,stype,tvalue,ttype,get_value=get_value)
        if sv==tv:
            return 0
        if sv < tv:
            return -1
        if sv > tv:
            return 1

