from distutils.core import setup
from Cython.Build import cythonize

setup(
    ext_modules=cythonize(["main/query.pyx", "crud/create.pyx", "crud/read.pyx", "conf/config.pyx"]),
)