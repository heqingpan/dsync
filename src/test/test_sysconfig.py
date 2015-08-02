# -*- coding:utf8 -*-

import os
import sys
_dir=os.path.realpath(os.path.dirname(__file__))
_basedir=os.path.dirname(_dir)
sys.path.append(_basedir)

from dsync import synchor
from dsync.typeinfo import TypeInfo
from dsync.builder import Builder
from dsync.generator import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import simplejson as json

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
    r=synchor.Core.sync_table(sconfig,tconfig)
    print r

def test_01():
    configstr='''{
    "sconn":{
        "type":"mysql",
        "host":"127.0.0.1",
        "port":"3306",
        "dbname":"bigagio",
        "user":"admin",
        "password":"admin",
        "charset":"utf8"
    },
    "tconn":{
        "type":"mysql",
        "host":"127.0.0.1",
        "port":"3306",
        "dbname":"test01",
        "user":"admin",
        "password":"admin",
        "charset":"utf8"
    },
    "group":[
        [
            {
                "table":"TB_SYSCONFIG",
                "keys":[{"name":"ID","type":"str"}],
                "fields":[
                    {"name":"ID","type":"str"},
                    {"name":"SYSKEY","type":"str"},
                    {"name":"NAME","type":"str"},
                    {"name":"VALUE","type":"str"}
                ],
                "where":""
            },
            {
                "table":"TB_SYSCONFIG",
                "keys":[{"name":"ID","type":"str"}],
                "fields":[
                    {"name":"ID","type":"str"},
                    {"name":"SYSKEY","type":"str"},
                    {"name":"NAME","type":"str"},
                    {"name":"VALUE","type":"str"}
                ],
                "where":""
            }
        ]
    ]
        
}'''
    #config=json.loads(configstr)
    #br=Builder.build(config)
    #print br
    #rlist=synchor.Core.sync_group(br,is_echo=True)
    #print rlist
    synchor.Core.sync_by_json(configstr,is_echo=True)

def test_02():
    config_file=os.path.join(_dir,"test_sysconfig_config.json")
    synchor.Core.sysc_by_file(config_file,is_echo=True)

def test_03():
    config_file=os.path.join(_dir,"test_sysconfig_config.json")
    fobj=open(config_file)
    config=json.load(fobj)
    fobj.close()
    sconnconfig=config["sconn"]
    tconnconfig=config["tconn"]
    out_file=os.path.join(_dir,"test_sysconfig_config.out")
    Generator.gene_config_to_file(sconnconfig,tconnconfig,out_file)

def test_04():
    config_file=os.path.join(_dir,"test_sysconfig_config.out")
    synchor.Core.sysc_by_file(config_file,is_echo=True)

def test_05():
    sconnstr=r"mysql+mysqldb://admin:admin@127.0.0.1:3306/bigagio?charset=utf8"
    tconnstr=r"mysql+mysqldb://admin:admin@127.0.0.1:3306/test01?charset=utf8"
    synchor.Core.sysc_by_path(sconnstr,tconnstr,is_echo=True)

def main():
    #test_00()
    #test_01()
    #test_02()
    #test_03()
    #test_04()
    test_05()


if __name__=="__main__":
    main()
