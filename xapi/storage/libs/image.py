from __future__ import absolute_import
import os.path


class Path(object):

    """An entity on the filesystem"""

    def __init__(self, path):
        self.path = os.path.realpath(path)


class Cow(Path):

    """An entity on the filesystem in cow format"""

    def __init__(self, path):
        Path.__init__(self, path)

    def format(self):
        return "vhd"

    def __str__(self):
        return "vhd:" + self.path


class Raw(Path):

    """An entity on the filesystem in raw format"""

    def __init__(self, path):
        Path.__init__(self, path)

    def format(self):
        return "raw"

    def __str__(self):
        return "aio:" + self.path
