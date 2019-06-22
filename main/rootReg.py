import os
import sys

dir = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(dir, '../'))

from crud import create

C = create.Create()

if __name__ == "__main__":

    rootPath = sys.argv[1]
    C.registerRoot(rootPath)

