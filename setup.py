from distutils.core import setup

setup(
    name='xcp-ng-xapi-storage-libs',
    version='1.0.2',
    description='XCP-ng implementation of the xapi-storage interface',
    author='XCP-ng team',
    author_email='contact@xcp-ng.com',
    url='https://github.com/xcp-ng/xcp-ng-xapi-storage',
    license='LGPLv2.1',
    packages=[
        'xapi.storage.libs',
        'xapi.storage.libs.libcow',
        'xapi.storage.libs.rrddlib'
    ]
)
