import os
import glob
import configparser
import json

config = configparser.ConfigParser()
config.read('../conf/db.conf')

class Create(object):

    def __init__(self):
        self.rootPath = ""
        self.db = ""
        self.tb = ""
        self.data = ""

    def registerRoot(self, rootPath):
        self.rootPath = rootPath
        if self.rootPath[-1:] == "/":
            self.rootPath = rootPath[:-1]
        wt = open("../conf/db.conf", 'w')
        config.set('tw_local', 'rootPath', self.rootPath)
        config.write(wt)
        wt.close()

        pathlist = self.rootPath.split("/")
        pathlink = "/"
        for path in pathlist:
            pathlink += path
            if not os.path.isdir(pathlink):
                os.mkdir(pathlink)
            pathlink += "/"

    def create(self, db, tb, data):

        self.rootPath = config['tw_local']['rootPath']

        pathlink = self.rootPath + "/" + db

        #db create
        if not os.path.isdir(pathlink):
            os.mkdir(pathlink)

        #tb create
        pathlink += "/" + tb
        if not os.path.isdir(pathlink):
            os.mkdir(pathlink)

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
