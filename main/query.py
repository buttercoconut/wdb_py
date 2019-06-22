import os
import sys
from traceback import format_exc
import json

dir = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(dir, '../'))

from crud import create, read, update, delete

C = create.Create()
R = read.Read()

if __name__ == "__main__":
    try:
        task = sys.argv[1]
        db = sys.argv[2]
        table = sys.argv[3]
        data = sys.argv[4]
    except:
        data = ""

    if task == "c":
        C.create(db, table, data)
    elif task == "r":
        if data:
            print("key 기준으로 value 조회")
            print(R.read_keyword(db, table, data))
            print("-----------------------------\nvalue 기준으로 rows 조회")
            print(R.read_value(db, table, data))
        elif data == "":
            print(R.read_all(db, table))
    elif task == "d":
        if data:
            print("deleted!!")
