import os, sys
import json

from multiprocessing import Process

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
        if not os.path.isdir(pathlink):
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

        total_data = R.read_all(db, tb)

        self.each_create(pathlink, total_data, idx, data)




    def bulk_create(self, db, tb, data_list):

        if isinstance(data_list, list):
            self.storePath = conf['db_store_path']

            pathlink = self.storePath + "/" + db + "/" + tb

            # mkdir path
            if not os.path.isdir(pathlink):
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

            idx_list = []
            for x in range(len(data_list)):
                idx = int(idx) + 1
                f = open(pathlink + "/_index", "w")
                f.write(str(idx))
                f.close()
                idx_list.append(idx)

            total_data = R.read_all(db, tb)

            proc_list = []

            for w in range(conf['workers']):
                front = int(len(data_list) * (w / conf['workers']))
                rear = int(len(data_list) * ((w + 1) / conf['workers']))
                proc = Process(target=self.each_create, args=(pathlink, total_data, idx_list[front:rear],
                                                              data_list[front:rear],))
                proc_list.append(proc)


            for p in proc_list:
                p.start()

            for p in proc_list:
                p.join()

        else:
            print("데이터가 올바르지 않습니다.")



    def each_create(self, pathlink, total_data, idx, data):

        if not isinstance(idx, list):
            t.update(json.loads(data))

            for key in t.keys():
                # insert_data = {idx: t[key]}
                insert_data = t[key]
                with open(pathlink + "/" + key, "w") as outfile:
                    try:
                        insert_data.update(total_data[key])
                    except:
                        pass
                    finally:
                        json.dump(insert_data, outfile)

        else:
            for each_idx, each in enumerate(data):

                t.update(each)

                for key in t.keys():
                    # insert_data = {idx[each_idx]: t[key]}
                    insert_data = t[key]
                    with open(pathlink + "/" + key, "w") as outfile:
                        try:
                            insert_data.update(total_data[key])
                        except:
                            pass
                        finally:
                            json.dump(insert_data, outfile)