#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging

from errno import EACCES
from os.path import realpath
from sys import argv, exit
from threading import Lock
from diskcache import Cache
from diskcache import FanoutCache
import hashlib
import os
import io
import json

cache = Cache('/cache', size_limit=int(120e9))

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


class Loopback(LoggingMixIn, Operations):
    def __init__(self, root):
        self.root = realpath(root)
        self.rwlock = Lock()

    def __call__(self, op, path, *args):
        return super(Loopback, self).__call__(op, self.root + path, *args)

    def access(self, path, mode):
        if not os.access(path, mode):
            raise FuseOSError(EACCES)

    chmod = os.chmod
    chown = os.chown

    def create(self, path, mode):
	
	print("CLEAR")
        return os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)

    def flush(self, path, fh):
        return os.fsync(fh)

    def fsync(self, path, datasync, fh):
        if datasync != 0:
          return os.fdatasync(fh)
        else:
          return os.fsync(fh)

    def getattr(self, path, fh=None):
        st = os.lstat(path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
            'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    getxattr = None

    def link(self, target, source):
        return os.link(source, target)

    listxattr = None
    mkdir = os.mkdir
    mknod = os.mknod
    open = os.open

    def read(self, path, size, offset, fh):
        with self.rwlock:
	    ATime = cache.get("mtime" + hashlib.sha224(path).hexdigest())
	    if ATime is None:
		print("MTIME MISS")
		cache.evict(hashlib.sha224(path).hexdigest())
		cache.expire()
		cache.set("mtime" + hashlib.sha224(path).hexdigest(), os.path.getmtime(path))
	    else:
		if ATime != os.path.getmtime(path):
			print("MTIME EVICT")
			cache.evict(hashlib.sha224(path).hexdigest())
			cache.expire()
		else:
			print("MTIME HIT")

	    A = cache.get(hashlib.sha224(path).hexdigest() + str(size) + str(offset))
	    if A is None:            
            	os.lseek(fh, offset, 0)
		B = os.read(fh, size)
		print("MISS " + hashlib.sha224(path).hexdigest())
		cache.set(hashlib.sha224(path).hexdigest() + str(size) + str(offset), B, tag=hashlib.sha224(path).hexdigest())
            	return B
	    else:
		print("HIT")
		return A		

    def readdir(self, path, fh):
        return ['.', '..'] + os.listdir(path)

    readlink = os.readlink

    def release(self, path, fh):
        return os.close(fh)

    def rename(self, old, new):
        return os.rename(old, self.root + new)

    rmdir = os.rmdir

    def statfs(self, path):
        stv = os.statvfs(path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def symlink(self, target, source):
        return os.symlink(source, target)

    def truncate(self, path, length, fh=None):
	
	print("CLEAR")
        with open(path, 'r+') as f:
            f.truncate(length)

    unlink = os.unlink
    utimens = os.utime

    def write(self, path, data, offset, fh):
        with self.rwlock:
	    
            print("CLEAR")
            os.lseek(fh, offset, 0)
            return os.write(fh, data)


if __name__ == '__main__':
    if len(argv) != 3:
        print('usage: %s <root> <mountpoint>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.INFO)

    fuse = FUSE(Loopback(argv[1]), argv[2], foreground=True)
