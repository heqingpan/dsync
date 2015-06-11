# -*- coding:utf8 -*-

import os
import sys
_dir=os.path.realpath(os.path.dirname(__file__))
_basedir=os.path.dirname(_dir)
sys.path.append(_basedir)

from dsync import synchor_tabel
from dsync.typeinfo import TypeInfo

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy

def build_config():
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

def test_00():
    sconfig,tconfig = build_config()
    synchor_tabel.Common.sync_table(sconfig,tconfig)

def main():
    test_00()


if __name__=="__main__":
    main()
