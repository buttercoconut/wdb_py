import os
import sys
import ast
from traceback import format_exc

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
    elif task == "bc":
        C.bulk_create(db, table, ast.literal_eval(data))
    elif task == "r":
        if data:
            t = R.read_keyword(db, table, data)
            print(dict(t))
        elif data == "":
            t = R.read_all(db, table)
            print(dict(t))
    elif task == "d":
        if data:
            print("deleted!!")
