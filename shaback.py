import hashlib
import os
import re
import shutil
import socket
import stat
import sys
import time
import xml.dom.minidom

from xml.sax import saxutils

sys.path.append("../s3c")
import s3lib

def hashfile(fn):
    hash = hashlib.sha1()
    f = open(fn)
    while True:
        buf = f.read(16384)
        if len(buf) == 0:
            break
        hash.update(buf)
    f.close()
    return hash.hexdigest()

class FileInfo:
    def __init__(self, **args):
        self.name = None
        self.size = None
        self.mtime = None
        self.mode = None
        self.uid = None
        self.gid = None
        self.hash = None
        if 'name' in args:
            st = os.stat(args['name'])
            self.name = args['name']
            self.size = st.st_size
            self.mtime = st.st_mtime
            self.mode = st.st_mode
            self.uid = st.st_uid
            self.gid = st.st_gid
        elif 'xml' in args:
            data = dict([(x, args['xml'].getElementsByTagName(x)[0].firstChild.data) for x in ("name", "size", "mtime", "mode", "uid", "gid", "hash")])
            self.name = data['name']
            self.size = int(data['size'])
            self.mtime = int(data['mtime'])
            self.mode = int(data['mode'])
            self.uid = int(data['uid'])
            self.gid = int(data['gid'])
            self.hash = data['hash']
        else:
            assert False
    def toxml(self):
        return (
            "  <fileinfo>\n" +
            "    <name>%s</name>\n" % saxutils.escape(self.name) +
            "    <size>%d</size>\n" % self.size +
            "    <mtime>%d</mtime>\n" % self.mtime +
            "    <mode>0%o</mode>\n" % self.mode +
            "    <uid>%d</uid>\n" % self.uid +
            "    <gid>%d</gid>\n" % self.gid +
            "    <hash>%s</hash>\n" % self.hash +
            "  </fileinfo>\n"
        )

def shellquote(s):
    return "'" + s.replace("'", "'\\''") + "'"

def putpipe(name, cmd):
    MAX_SIZE = 1000000
    p = os.popen(cmd)
    data = p.read(MAX_SIZE)
    tf = None
    if len(data) >= MAX_SIZE:
        tf = os.tmpfile()
        tf.write(data)
        shutil.copyfileobj(p, tf)
        data = tf
    r = p.close()
    if r is not None:
        print >>sys.stderr, "shaback: Error processing file %s: %s" % (fn, r)
    else:
        r = s3.put(name, data)
    if tf is not None:
        tf.close()

def walktree(base, callback):
    for f in os.listdir(base):
        path = os.path.join(base, f)
        mode = os.stat(path).st_mode
        if stat.S_ISDIR(mode):
            walktree(path, callback)
        elif stat.S_ISREG(mode):
            callback(path)
        else:
            print "Skipping", path

def backup(path):
    refname = "shaback-" + socket.gethostname() + "-" + re.sub(re.escape(os.sep), "#", os.path.abspath(path))
    print refname
    start = time.localtime(time.time())
    lastfiles = {}
    try:
        doc = xml.dom.minidom.parse(os.path.join(os.environ['HOME'], ".shaback", "refs", refname))
        for fi in doc.getElementsByTagName("fileinfo"):
            fn = fi.getElementsByTagName("name")[0].firstChild.data
            lastfiles[fn] = FileInfo(xml = fi)
        doc.unlink()
    except IOError:
        pass
    print "Scanning files"
    files = []
    walktree(path, lambda x: files.append(FileInfo(name = x)))
    print "Total: %d files, %d bytes" % (len(files), sum([x.size for x in files]))
    hashfiles = []
    if False: # rehash
        hashfiles = files
    else:
        for fi in files:
            if fi.name in lastfiles and fi.mtime == lastfiles[fi.name].mtime and fi.size == lastfiles[fi.name].size:
                fi.hash = lastfiles[fi.name].hash
            else:
                hashfiles.append(fi)
    print "To hash: %d files, %d bytes" % (len(hashfiles), sum([x.size for x in hashfiles]))
    for fi in hashfiles:
        print "hashing", fi.name
        hash = hashfile(fi.name)
        if fi.hash is not None and hash != fi.hash:
            print >>sys.stderr, "Warning: file %s had same mtime and size, but hash did not match" % fi.name
        fi.hash = hash
    blobdir = s3.list("shaback.hewgill.com", "?prefix=blob/")
    hashlen = hashlib.sha1().digest_size * 2
    blobs = frozenset([x['Key'][5:5+hashlen] for x in blobdir['Contents']])
    print "Uploading file data"
    for fi in [x for x in files if x.hash not in blobs]:
        print fi.name
        putpipe("shaback.hewgill.com/blob/"+fi.hash+".bz2", "bzip2 <"+shellquote(fi.name))
    print "Writing index"
    f = open(os.path.join(os.environ['HOME'], ".shaback", "refs", refname+"-"+time.strftime("%Y%m%d-%H%M%S", start)), "w")
    print >>f, """<?xml version="1.0"?>"""
    print >>f, "<shaback>"
    for fi in files:
        f.write(fi.toxml())
    print >>f, "</shaback>"
    f.close()
    s3.put("shaback.hewgill.com/refs/"+refname+"-"+time.strftime("%Y%m%d-%H%M%S", start), file(os.path.join(os.environ['HOME'], ".shaback", "refs", refname+"-"+time.strftime("%Y%m%d-%H%M%S", start))))
    try:
        os.unlink(os.path.join(os.environ['HOME'], ".shaback", "refs", refname))
    except:
        pass
    os.symlink(refname+"-"+time.strftime("%Y%m%d-%H%M%S", start), os.path.join(os.environ['HOME'], ".shaback", "refs", refname))

def fsck():
    pass

def gc():
    pass

def restore(path):
    pass

def usage():
    print >>sys.stderr, "Usage: shaback backup path"
    print >>sys.stderr, "       shaback fsck"
    print >>sys.stderr, "       shaback gc"
    print >>sys.stderr, "       shaback restore path"
    sys.exit(1)

if len(sys.argv) < 2:
    usage()

access = None
secret = None
s3 = s3lib.S3Store(access, secret)

if sys.argv[1] == "backup":
    if len(sys.argv) == 3:
        backup(sys.argv[2])
    else:
        usage()
elif sys.argv[1] == "fsck":
    if len(sys.argv) == 2:
        fsck()
    else:
        usage()
elif sys.argv[1] == "gc":
    if len(sys.argv) == 2:
        gc()
    else:
        usage()
elif sys.argv[1] == "restore":
    if len(sys.argv) == 3:
        restore(sys.argv[2])
    else:
        usage()
else:
    usage()
