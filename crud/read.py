# -*- coding: utf-8 -*-
import os, sys
import glob
from BTrees.OOBTree import OOBTree
t = OOBTree()
import json

dir = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(dir, '../'))

from conf import config
conf = config.defult

class Read(object):

    def __init__(self):
        self.storePath = ""
        self.db = ""
        self.tb = ""
        self.data = ""

    def read_all(self, db, tb):

        self.storePath = conf['db_store_path']
        pathlink = self.storePath + "/" + db + "/" + tb

        datalist = glob.glob(pathlink + "/*")
        datalist.remove(pathlink + "/_index")

        for datapath in datalist:

            try:
                with open(datapath, "r") as readfile:
                    data = json.load(readfile)
                    t[datapath.split('/')[-1]] = data
            except:
                pass

        return t

    # DB내의 특정테이블의 특정 key값의 모든 데이터를 조회
    def read_keyword(self, db, tb, key):

        self.storePath = conf['db_store_path']
        pathlink = self.storePath + "/" + db + "/" + tb

        try:
            datapath = pathlink + "/" + key

            with open(datapath, "r") as readfile:
                data = json.load(readfile)
                t[datapath.split('/')[-1]] = data

        except:
            pass

        return t

    # DB내의 특정테이블의 특정 value값의 모든 데이터를 조회
    def read_value(self, db, tb, value):
        result = {}

        self.storePath = conf['db_store_path']
        pathlink = self.storePath + "/" + db + "/" + tb

        datalist = glob.glob(pathlink + "/*")
        datalist.remove(pathlink + "/_index")

        listOfIndex = []

        # value index search
        for datapath in datalist:

            f = open(datapath, "r")
            low_data = f.read()

            data_sep = low_data.split('$$$')
            for each in data_sep:
                try:
                    if value in each:
                        listOfIndex.append(each.split('@@@')[0])
                except IndexError:
                    pass
            f.close()


        # choosed index of rows
        for datapath in datalist:

            f = open(datapath, "r")
            low_data = f.read()
            try:
                result[value]
            except:
                result[value] = {}
                for each_idx in listOfIndex:
                    result[value][each_idx] = {}
            finally:
                data_sep = low_data.split('$$$')
                for each in data_sep:
                    try:
                        for idx in listOfIndex:
                            if idx == each.split('@@@')[0]:
                                result[value][each.split('@@@')[0]][datapath.split('/')[-1]] = each.split('@@@')[1]
                    except IndexError:
                        pass

            f.close()

        return result