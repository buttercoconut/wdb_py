import os, sys
import glob
import json

dir = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(dir, '../'))

from conf import config
conf = config.defult

class Create(object):

    def __init__(self):
        self.storePath = ""
        self.db = ""
        self.tb = ""
        self.data = ""

    def create(self, db, tb, data):

        self.storePath = conf['db_store_path']

        pathlink = self.storePath + "/" + db + "/" + tb

        # mkdir path
        stack_dir = ""
        for each_dir in pathlink.split("/")[1:]:
            stack_dir += "/" + each_dir
            if not os.path.isdir(stack_dir):
                os.mkdir(stack_dir)

        idx = 0
        try:
            f = open(pathlink + "/_index", "r")
            idx = f.read()
            f.close()
            idx = int(idx) + 1
        except:
            idx = int(idx) + 1
        finally:
            f = open(pathlink + "/_index", "w")
            f.write(str(idx))
            f.close()

        data_dict = dict(json.loads(data))

        for key in data_dict.keys():
            f = open(pathlink + "/" + key, "a")
            f.write(str(idx) + "@@@" + str(data_dict[key]) + "$$$")
            f.close()
