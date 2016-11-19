#!/bin/env python
# -*- coding: utf-8 -*-
from kazoo.client import KazooClient

import sys
import os
import json
import time
reload(sys)
sys.setdefaultencoding('utf-8')

print "BDA init scripts..."

ZK_PROBE_PATH = "/baseline4/input/probes"
ZK_APIS_PATH = "/baseline4/input/apis"
ZK_CATE_PATH = "/baseline4/input/api_categorys"
ZK_CATE_FILTERS_PATH = "/baseline4/input/cat_filters"
DATA_DIR = '%s/data' % sys.path[0]
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR, mode=0755)

zk_address = '172.24.5.95:2181'
zookeeper_op = KazooClient(hosts=zk_address)
zookeeper_op.start()

def import_probes():
    probe_file = '%s%s%s' % (DATA_DIR, os.sep, 'probes.txt')
    fp = open(probe_file, 'r')
    line = fp.readline()
    line.strip()
    while(line):
        json_value = json.loads(line)
        json_value['create_stamp'] = time.time()
        json_value['modify_stamp'] = json_value['create_stamp']
        appkey = json_value.get('appkey', '')
        if appkey:
            probe_path = '%s/%s' % (ZK_PROBE_PATH, appkey)
            if not zookeeper_op.exists(probe_path):
                zookeeper_op.create(probe_path,value = json.dumps(json_value), makepath = True)
            else:
                zookeeper_op.set(probe_path, value = json.dumps(json_value))


        line = fp.readline()
        line.strip()
    fp.close()


def export_probes():
    probe_file = '%s%s%s' % (DATA_DIR, os.sep, 'probes.txt')
    fp = open(probe_file, 'wb')

    probes = zookeeper_op.get_children(ZK_PROBE_PATH)
    for probe in probes:
            path = '%s/%s' % (ZK_PROBE_PATH, probe)
            value = zookeeper_op.get(path)
            json_value = json.loads(value[0])
            appkey = json_value.get('appkey', '')
            if appkey:
                fp.write(json.dumps(json_value, ensure_ascii=False))
                fp.write('\n')

    fp.close()

def import_categorys():
    category_file = '%s%s%s' % (DATA_DIR, os.sep, 'categorys.txt')
    fp = open(category_file, 'r')

    value = fp.readline()

    if not zookeeper_op.exists(ZK_CATE_PATH):
        zookeeper_op.create(ZK_CATE_PATH,value = value, makepath = True)
    else:
        zookeeper_op.set(ZK_CATE_PATH, value = value)

    fp.close()

def export_categorys():
    category_file = '%s%s%s' % (DATA_DIR, os.sep, 'categorys.txt')
    fp = open(category_file, 'wb')

    value = zookeeper_op.get(ZK_CATE_PATH)

    json_value = json.loads(value[0])
    if json_value:
        fp.write(json.dumps(json_value, ensure_ascii=False))
        fp.write('\n')

    fp.close()

def import_apis():
    api_file = '%s%s%s' % (DATA_DIR, os.sep, 'apis.txt')
    fp = open(api_file, 'r')
    line = fp.readline()
    line.strip()
    while(line):
        json_value = json.loads(line)
        json_value['create_stamp'] = time.time()
        json_value['modify_stamp'] = json_value['create_stamp']
        name = json_value.get('name', '')
        if name:
            api_path = '%s/%s' % (ZK_APIS_PATH, name)
            if not zookeeper_op.exists(api_path):
                zookeeper_op.create(api_path,value = json.dumps(json_value), makepath = True)
            else:
                zookeeper_op.set(api_path, value = json.dumps(json_value))


        line = fp.readline()
        line.strip()
    fp.close()

def export_apis():
    api_file = '%s%s%s' % (DATA_DIR, os.sep, 'apis.txt')
    fp = open(api_file, 'wb')

    apis = zookeeper_op.get_children(ZK_APIS_PATH)
    for api in apis:
            path = '%s/%s' % (ZK_APIS_PATH, api)
            value = zookeeper_op.get(path)
            json_value = json.loads(value[0])
            name = json_value.get('name', '')
            if name:
                fp.write(json.dumps(json_value, ensure_ascii=False))
                fp.write('\n')

    fp.close()


def import_cat_filters():
    #cat_filters_file = '%s%s%s' % (DATA_DIR, os.sep, 'cat_filters.txt')
    #fp = open(cat_filters_file, 'r')

    #value = fp.readline()

    value = '[]'
    if not zookeeper_op.exists(ZK_CATE_FILTERS_PATH):
        zookeeper_op.create(ZK_CATE_FILTERS_PATH,value = value, makepath = True)
    else:
        zookeeper_op.set(ZK_CATE_FILTERS_PATH, value = value)


    #fp.close()

def export_cat_filters():
    cat_filters_file = '%s%s%s' % (DATA_DIR, os.sep, 'cat_filters.txt')
    fp = open(cat_filters_file, 'wb')

    value = zookeeper_op.get(ZK_CATE_FILTERS_PATH)

    json_value = json.loads(value[0])
    if json_value:
        fp.write(json.dumps(json_value, ensure_ascii=False))
        fp.write('\n')

    fp.close()


def export_zk():
    #export_probes()
    export_categorys()
    export_apis()
    #export_cat_filters()


def import_zk():
    if not zookeeper_op.exists(ZK_APIS_PATH):
        zookeeper_op.create(ZK_APIS_PATH, makepath=True)

    if not zookeeper_op.exists(ZK_PROBE_PATH):
        zookeeper_op.create(ZK_PROBE_PATH, makepath=True)

    if not zookeeper_op.exists(ZK_CATE_PATH):
        zookeeper_op.create(ZK_CATE_PATH, makepath=True)

    if not zookeeper_op.exists(ZK_CATE_FILTERS_PATH):
        zookeeper_op.create(ZK_CATE_FILTERS_PATH, makepath=True)

    #import_probes()
    import_categorys()
    import_apis()
    import_cat_filters()

if '__main__' == __name__:
    if 2 != len(sys.argv):
        print "Usage: python init_zk_data export|import"
        sys.exit(0)

    aciton = sys.argv[1]
    if aciton not in ['export', 'import']:
        print "Usage: python init_zk_data export|import"
        sys.exit(0)

    if 'export' == aciton:
        export_zk()
        print "export done"

    else:
        import_zk()
        print "import done"
