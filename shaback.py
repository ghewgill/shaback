import cPickle
import hashlib
import os
import re
import shutil
import socket
import stat
import sys
import tempfile
import time
import xml.sax

from xml.sax import saxutils

sys.path.append("../s3c")
import s3lib

class Config:
    def __init__(self):
        self.Bucket = None
        self.DryRun = False
        self.Encrypt = None
        self.Exclude = []

Config = Config()
s3 = None

def readConfig():
    fn = ".shabackrc"
    if 'SHABACKRC' in os.environ:
        fn = os.environ['SHABACKRC']
    elif 'HOME' in os.environ:
        fn = os.path.join(os.environ['HOME'], fn)
    elif 'HOMEDRIVE' in os.environ and 'HOMEPATH' in os.environ:
        fn = os.path.join(os.environ['HOMEDRIVE'], os.environ['HOMEPATH'], fn)
    f = None
    try:
        f = open(fn)
        for s in f:
            m = re.match(r"(\w+)\s+(\S+)", s)
            if m is None:
                continue
            if m.group(1) == "bucket":
                Config.Bucket = m.group(2)
            else:
                continue
        f.close()
    except:
        if f is not None:
            f.close()

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
    def saxHandler(self, name, data):
        if   name == "name" : self.name  = data
        elif name == "size" : self.size  = int(data)
        elif name == "mtime": self.mtime = int(data)
        elif name == "mode" : self.modde = int(data) # octal!
        elif name == "uid"  : self.uid   = int(data)
        elif name == "gid"  : self.gid   = int(data)
        elif name == "hash" : self.hash  = data
        else:
            print >>sys.stderr, "Unknown field:", name
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

class RefsHandler(xml.sax.ContentHandler):
    def __init__(self, files):
        self.files = files
        self.element = None
        self.fileinfo = None
        self.text = ""
    def startElement(self, name, attrs):
        self.element = name
        if name == "fileinfo":
            self.fileinfo = FileInfo()
            self.element = None
        self.text = ""
    def endElement(self, name):
        if name == "fileinfo":
            self.files.append(self.fileinfo)
            self.fileinfo = None
        elif self.fileinfo is not None and self.element is not None:
            self.fileinfo.saxHandler(self.element, self.text)
        self.element = None
    def characters(self, content):
        self.text += content

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
    shabackpath = os.path.join(os.environ['HOME'], ".shaback")
    refpath = os.path.join(shabackpath, "refs")
    refname = "shaback-" + socket.gethostname() + "-" + re.sub(re.escape(os.sep), "#", os.path.abspath(path))
    print refname
    start = time.localtime(time.time())
    lastfiles = {}
    try:
        files = []
        xml.sax.parse(os.path.join(refpath, refname+".xml"), RefsHandler(files))
        for fi in files:
            lastfiles[fi.name] = fi
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
    print "Reading blob cache"
    blobs = {}
    f = None
    try:
        f = open(os.path.join(shabackpath, "blobcache"))
        blobs = cPickle.load(f)
    except:
        pass
    finally:
        if f is not None:
            f.close()
    print "Uploading file data"
    for fi in [x for x in files if x.hash not in blobs]:
        print fi.name
        suffix = ".bz2"
        if Config.Encrypt:
            suffix += ".gpg"
        blobs[fi.hash] = suffix
        fn = Config.Bucket + "/blob/" + fi.hash + suffix
        try:
            # head is 10x cheaper than list
            s3.get(fn, method = "HEAD")
        except s3lib.S3Exception:
            cmd = "bzip2 <" + shellquote(fi.name)
            if Config.Encrypt:
                cmd += " | gpg --encrypt -r " + Config.Encrypt
            if not Config.DryRun:
                putpipe(fn, cmd)
    print "Writing blob cache"
    if not Config.DryRun:
        f = open(os.path.join(shabackpath, "blobcache"), "w")
        cPickle.dump(blobs, f)
        f.close()
    print "Writing index"
    timestamp = "-" + time.strftime("%Y%m%d-%H%M%S", start)
    f = open(os.path.join(refpath, refname+timestamp+".xml"), "w")
    print >>f, """<?xml version="1.0"?>"""
    print >>f, "<shaback>"
    for fi in files:
        f.write(fi.toxml())
    print >>f, "</shaback>"
    f.close()
    fn = Config.Bucket + "/refs/" + refname + timestamp + ".xml.bz2"
    cmd = "bzip2 <" + shellquote(os.path.join(refpath, refname + timestamp + ".xml"))
    if Config.Encrypt:
        fn += ".gpg"
        cmd += " | gpg --encrypt -r " + Config.Encrypt
    if not Config.DryRun:
        putpipe(fn, cmd)
        try:
            os.unlink(os.path.join(refpath, refname+".xml"))
        except:
            pass
        os.symlink(refname+timestamp+".xml", os.path.join(refpath, refname+".xml"))

def fsck():
    print "Reading blobs"
    blobdir = s3.list(Config.Bucket, "?prefix=blob/")
    hashlen = hashlib.sha1().digest_size * 2
    blobs = frozenset([x['Key'][5:5+hashlen] for x in blobdir['Contents']])
    print "%d blobs found" % len(blobs)
    print "Reading refs"
    refsdir = s3.list(Config.Bucket, "?prefix=refs/")
    badrefs = set()
    badfiles = set()
    for r in [x['Key'] for x in refsdir['Contents']]:
        print r
        f = s3.get(Config.Bucket+"/"+r)
        tfh, tfn = tempfile.mkstemp(prefix = "shaback.")
        p = os.popen("bunzip2 >"+shellquote(tfn), "wb")
        try:
            shutil.copyfileobj(f, p)
            p.close()
            try:
                files = []
                xml.sax.parse(tfn, RefsHandler(files))
            except xml.sax.SAXException:
                print "Warning: failed to read refs file:", r
                continue
            for fi in files:
                if fi.hash not in blobs:
                    print "Blob %s referenced from %s (%s) not found!" % (fi.hash, r, fi.name)
                    badrefs.add(r)
                    badfiles.add(fi.name)
        finally:
            os.close(tfh)
            os.unlink(tfn)
    if len(badrefs) > 0:
        print
        print "Reference files with missing blobs:"
        for r in sorted(badrefs):
            print r
    if len(badfiles) > 0:
        print
        print "Files with missing blobs:"
        for fn in sorted(badfiles):
            print fn

def gc():
    print "Reading blobs"
    blobdir = s3.list(Config.Bucket, "?prefix=blob/")
    hashlen = hashlib.sha1().digest_size * 2
    blobs = dict([(x['Key'][5:5+hashlen], x['Key']) for x in blobdir['Contents']])
    print "%d blobs found" % len(blobs)
    failedrefs = False
    print "Reading refs"
    refsdir = s3.list(Config.Bucket, "?prefix=refs/")
    for r in [x['Key'] for x in refsdir['Contents']]:
        print r
        f = s3.get(Config.Bucket+"/"+r)
        tfh, tfn = tempfile.mkstemp(prefix = "shaback.")
        p = os.popen("bunzip2 >"+shellquote(tfn), "wb")
        try:
            shutil.copyfileobj(f, p)
            p.close()
            try:
                files = []
                xml.sax.parse(tfn, RefsHandler(files))
            except xml.sax.SAXException:
                failedrefs = True
                continue
            for fi in files:
                if fi.hash in blobs:
                    del blobs[fi.hash]
        finally:
            os.close(tfh)
            os.unlink(tfn)
    if not failedrefs:
        print "%d unreferenced blobs to delete" % len(blobs)
        if not Config.DryRun:
            for b in blobs.values():
                s3.delete(Config.Backup+"/"+b)
    else:
        print "Failed to read one or more refs files, not deleting anything"

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
command = None
args = []

readConfig()

a = 1
while a < len(sys.argv):
    if sys.argv[a][0] == "-":
        if sys.argv[a] == "-a" or sys.argv[a] == "--access":
            a += 1
            access = sys.argv[a]
        elif sys.argv[a] == "-s" or sys.argv[a] == "--secret":
            a += 1
            secret = sys.argv[a]
        elif sys.argv[a] == "--dry-run":
            Config.DryRun = True
        elif sys.argv[a] == "--encrypt":
            a += 1
            Config.Encrypt = sys.argv[a]
        elif sys.argv[a] == "--exclude":
            a += 1
            Config.Exclude += [sys.argv[a]]
        else:
            print >>sys.stderr, "shaback: Unknown option:", sys.argv[a]
            sys.exit(1)
    else:
        if command is None:
            command = sys.argv[a]
        else:
            args.append(sys.argv[a])
    a += 1

if Config.Bucket is None:
    print >>sys.stderr, "shaback: No bucket specified (--bucket or ~/.shabackrc)"
    usage()

s3 = s3lib.S3Store(access, secret)

if command == "backup":
    if len(args) == 1:
        backup(args[0])
    else:
        usage()
elif command == "fsck":
    if len(args) == 0:
        fsck()
    else:
        usage()
elif command == "gc":
    if len(args) == 0:
        gc()
    else:
        usage()
elif command == "restore":
    if len(args) == 1:
        restore(args[0])
    else:
        usage()
else:
    usage()
