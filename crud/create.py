import os, sys
import json

from BTrees.OOBTree import OOBTree
t = OOBTree()

dir = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(dir, '../'))

from crud import read
R = read.Read()

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
        except:
            pass
        finally:
            idx = int(idx) + 1
            f = open(pathlink + "/_index", "w")
            f.write(str(idx))
            f.close()

        t.update(json.loads(data))
        total_data = R.read_all(db, tb)

        for key in t.keys():
            insert_data = {idx: t[key]}
            with open(pathlink + "/" + key, "w") as outfile:
                insert_data.update(total_data[key])
                json.dump(insert_data, outfile)
