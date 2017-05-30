import os
from setuptools import setup, find_packages
from distutils.command.clean import clean as _clean
from distutils.command.build_py import build_py as _build_py
from distutils.spawn import find_executable
import subprocess
import sys


project_root = os.path.abspath(os.path.dirname(__file__))


with open(os.path.join(project_root, 'README.md')) as fh:
    README = fh.read()


if 'PROTOC' in os.environ and os.path.exists(os.environ['PROTOC']):
    protoc = os.environ['PROTOC']
else:
    protoc = find_executable("protoc")


REQUIREMENTS = [
    'kafka',
    'protobuf',
]


# map of src -> dst mappings. former should be *.proto, latter *_pb2.py.
proto_source = {
    '../../proto/segment.proto': 'otdatastore/segment_pb2.py',
}


def generate_proto(src, dst):
    if (not os.path.exists(dst) or
        os.path.exists(src) and
        (os.path.getmtime(src) > os.path.getmtime(dst))):

        src_dir = os.path.dirname(src)
        dst_dir = os.path.dirname(dst)
        protoc_cmd = [ protoc, "-I" + src_dir, "--python_out=" + dst_dir,
                       src ]

        if subprocess.call(protoc_cmd) != 0:
            sys.exit(-1)


class build_py(_build_py):
    def run(self):
        for src, dst in proto_source.items():
            generate_proto(src, dst)
        _build_py.run(self)


class clean(_clean):
    def run(self):
        for (dirpath, dirnames, filenames) in os.walk("."):
            for filename in filenames:
                if filename.endswith("_pb2.py"):
                    os.remove(os.path.join(dirpath, filename))
        _clean.run(self)


setup(
    name='otdatastore',
    version='0.0.1',
    description='OpenTraffic Datastore',
    long_description=README,
    author='Matt Amos',
    author_email='matt.amos@mapzen.com',
    url='https://github.com/opentraffic/datastore',
    license='LGPLv3',
    install_requires=REQUIREMENTS,
    keywords=['opentraffic'],
    packages=find_packages(exclude='test'),
    entry_points={
        'console_scripts': [
            'opentraffic-datastore-frontend=otdatastore.frontend:main'
        ],
    },
    cmdclass={
        'clean': clean,
        'build_py': build_py,
    },
    test_suite='test',
)
