import cPickle
import fnmatch
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

sys.path.append("s3lib")
import s3lib

class Config:
    def __init__(self):
        self.Bucket = None
        self.DryRun = False
        self.Encrypt = None
        self.Passphrase = None
        self.Verbose = False
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
            elif m.group(1) == "exclude":
                Config.Exclude.append(m.group(2))
            else:
                continue
        f.close()
    except:
        if f is not None:
            f.close()

def hashfile(fn, issymlink = False):
    hash = hashlib.sha1()
    if issymlink:
        try:
            hash.update(os.readlink(fn))
        except IOError, e:
            print >>sys.stderr, "Error (%s): %s" % (e, fn)
            return None
    else:
        try:
            f = open(fn)
        except IOError, e:
            print >>sys.stderr, "Error (%s): %s" % (e, fn)
            return None
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
            assert 'stat' in args
            self.name = args['name']
            self.size = args['stat'].st_size
            self.mtime = args['stat'].st_mtime
            self.mode = args['stat'].st_mode
            self.uid = args['stat'].st_uid
            self.gid = args['stat'].st_gid
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

def putpipe(name, cmd, path):
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
        print >>sys.stderr, "shaback: Error processing file %s: %s" % (path, r)
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
    try:
        files = os.listdir(base)
    except OSError, e:
        print >>sys.stderr, "Error (%s): %s" % (e, base)
        return
    for f in files:
        path = os.path.join(base, f)
        try:
            st = os.lstat(path)
        except OSError, e:
            print >>sys.stderr, "Error (%s): %s" % (e, path)
            continue
        if stat.S_ISDIR(st.st_mode):
            walktree(path, callback)
        elif stat.S_ISREG(st.st_mode):
            callback(path, st)
        elif stat.S_ISLNK(st.st_mode):
            callback(path, st)
        else:
            print "Skipping", path

def progress(**args):
    sys.stdout.write(" %d\r" % args['count'])
    sys.stdout.flush()

def backup(path):
    shabackpath = os.path.join(os.environ['HOME'], ".shaback")
    if not os.access(shabackpath, os.F_OK):
        os.mkdir(shabackpath)
    refpath = os.path.join(shabackpath, "refs")
    if not os.access(refpath, os.F_OK):
        os.mkdir(refpath)
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
    excluded = []
    def addfile(fn, st):
        for e in Config.Exclude:
            if fnmatch.fnmatch(fn, e):
                if Config.Verbose:
                    print "exclude", fn
                excluded.append(fn)
                return
        files.append(FileInfo(name = fn, stat = st))
    walktree(path, addfile)
    print "Total: %d files, %d bytes" % (len(files), sum([x.size for x in files])), "(%d excluded)" % len(excluded) if excluded else ""
    hashfiles = []
    if False: # rehash
        hashfiles = files
    else:
        for fi in files:
            if fi.name in lastfiles and fi.mtime == lastfiles[fi.name].mtime and fi.size == lastfiles[fi.name].size:
                fi.hash = lastfiles[fi.name].hash
            else:
                hashfiles.append(fi)
    total = sum([x.size for x in hashfiles])
    print "To hash: %d files, %d bytes" % (len(hashfiles), total)
    done = 0 
    for fi in hashfiles:
        if Config.Verbose:
            print "hashing", fi.name
        hash = hashfile(fi.name, stat.S_ISLNK(fi.mode))
        if hash is None:
            continue
        if fi.hash is not None and hash != fi.hash:
            print >>sys.stderr, "Warning: file %s had same mtime and size, but hash did not match" % fi.name
        fi.hash = hash
        done += fi.size
        if sys.stdout.isatty():
            sys.stdout.write("%3d%%\r" % int(100*done/total))
            sys.stdout.flush()
            if Config.Verbose:
                print
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
    files = [x for x in files if x.hash is not None]
    print "Uploading file data"
    todo = [x for x in files if x.hash is not None and x.hash not in blobs]
    total = sum([x.size for x in todo])
    print "To upload: %d files, %d bytes" % (len(todo), total)
    cachecount = 0
    done = 0
    for fi in todo:
        if Config.Verbose:
            print fi.name
        suffix = ".bz2"
        if Config.Encrypt:
            suffix += ".gpg"
        blobs[fi.hash] = suffix
        fn = Config.Bucket + "/blob/" + fi.hash + suffix
        try:
            # head is 10x cheaper than list
            s3.get(fn, method = "HEAD")
            if Config.Verbose:
                print "- blob already present on backup"
        except s3lib.S3Exception:
            if stat.S_ISLNK(fi.mode):
                cmd = "echo -n " + shellquote(os.readlink(fi.name)) + " | bzip2"
            else:
                cmd = "bzip2 <" + shellquote(fi.name)
            if Config.Encrypt:
                cmd += " | gpg --encrypt --no-armor -r " + Config.Encrypt
            if not Config.DryRun:
                putpipe(fn, cmd, fi.name)
        done += fi.size
        cachecount += 1
        if cachecount > 100:
            if not Config.DryRun:
                if Config.Verbose:
                    print "Rewriting blobcache"
                f = open(os.path.join(shabackpath, "blobcache"), "w")
                cPickle.dump(blobs, f)
                f.close()
            cachecount = 0
        if sys.stdout.isatty():
            sys.stdout.write("%3d%%\r" % int(100*done/total))
            sys.stdout.flush()
            if Config.Verbose:
                print
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
        cmd += " | gpg --encrypt --no-armor -r " + Config.Encrypt
    if not Config.DryRun:
        putpipe(fn, cmd, os.path.join(refpath, refname + timestamp + ".xml"))
        try:
            os.unlink(os.path.join(refpath, refname+".xml"))
        except:
            pass
        os.symlink(refname+timestamp+".xml", os.path.join(refpath, refname+".xml"))

def getfile(name, allowunencryptable):
    process = True
    filters = []
    for suffix in reversed(name.split(".")):
        if suffix == "bz2":
            filters.append("bunzip2")
        elif suffix == "gpg":
            if Config.Passphrase is None:
                print >>sys.stderr, "Encrypted file found and no passphrase specified:", name
                if allowunencryptable:
                    process = False
                    break
                else:
                    sys.exit(1)
            # TODO: use --passphrase-fd
            filters.append("gpg --decrypt --passphrase %s" % shellquote(Config.Passphrase))
        else:
            break
    if not process:
        return None
    f = s3.get(Config.Bucket+"/"+name)
    cmd = "|".join(filters)
    (pipein, pipeout) = os.popen2(cmd)
    if os.fork() == 0:
        shutil.copyfileobj(f, pipein)
        sys.exit(0)
    pipein.close()
    return pipeout

def reffiles(allowunencryptable):
    refsdir = s3.list(Config.Bucket, "?prefix=refs/")
    for r in [x['Key'] for x in refsdir['Contents']]:
        yield (r, getfile(r, allowunencryptable))

def fsck():
    print "Reading blobs"
    blobdir = s3.list(Config.Bucket, "?prefix=blob/", callback = progress)
    hashlen = hashlib.sha1().digest_size * 2
    blobs = frozenset([x['Key'][5:5+hashlen] for x in blobdir['Contents']])
    print "%d blobs found" % len(blobs)
    print "Reading refs"
    badrefs = set()
    badfiles = set()
    for r, f in reffiles(True):
        if f is None:
            continue
        if Config.Verbose:
            print r
        try:
            files = []
            xml.sax.parse(f, RefsHandler(files))
        except xml.sax.SAXException:
            print "Warning: failed to read refs file:", r
            continue
        finally:
            f.close()
        for fi in files:
            if fi.hash not in blobs:
                print "Blob %s referenced from %s (%s) not found!" % (fi.hash, r, fi.name)
                badrefs.add(r)
                badfiles.add(fi.name)
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
    blobdir = s3.list(Config.Bucket, "?prefix=blob/", callback = progress)
    hashlen = hashlib.sha1().digest_size * 2
    blobs = dict([(x['Key'][5:5+hashlen], x['Key']) for x in blobdir['Contents']])
    print "%d blobs found" % len(blobs)
    failedrefs = False
    print "Reading refs"
    for r, f in reffiles(False):
        if Config.Verbose:
            print r
        try:
            files = []
            xml.sax.parse(f, RefsHandler(files))
        except xml.sax.SAXException:
            failedrefs = True
            continue
        finally:
            f.close()
        for fi in files:
            if fi.hash in blobs:
                del blobs[fi.hash]
    if not failedrefs:
        print "%d unreferenced blobs to delete" % len(blobs)
        if not Config.DryRun:
            for b in blobs.values():
                s3.delete(Config.Backup+"/"+b)
    else:
        print "Failed to read one or more refs files, not deleting anything"

def refresh():
    shabackpath = os.path.join(os.environ['HOME'], ".shaback")
    print "Reading blobs"
    blobdir = s3.list(Config.Bucket, "?prefix=blob/", callback = progress)
    hashlen = hashlib.sha1().digest_size * 2
    blobs = dict([(x['Key'][5:5+hashlen], x['Key'][5+hashlen:]) for x in blobdir['Contents']])
    print "%d blobs found" % len(blobs)
    print "Writing blob cache"
    if not Config.DryRun:
        f = open(os.path.join(shabackpath, "blobcache"), "w")
        cPickle.dump(blobs, f)
        f.close()

def restore(path):
    pass

def verify():
    print "Reading blobs"
    blobdir = s3.list(Config.Bucket, "?prefix=blob/", callback = progress)
    hashlen = hashlib.sha1().digest_size * 2
    total = sum([int(x['Size']) for x in blobdir['Contents']])
    done = 0 
    for f in blobdir['Contents']:
        if Config.Verbose:
            print "reading", f['Key'], "(%s)" % f['Size']
        data = getfile(f['Key'], True)
        if data is not None:
            h = hashlib.sha1()
            while True:
                buf = data.read(16384)
                if len(buf) == 0:
                    break
                h.update(buf)
            data.close()
            hash = h.hexdigest()
            if f['Key'][5:5+hashlen] != hash:
                print "Hash verification error:", f['Key'], "actual", hash
        done += int(f['Size'])
        if sys.stdout.isatty():
            sys.stdout.write("%3d%%\r" % int(100*done/total))
            sys.stdout.flush()
            if Config.Verbose:
                print

def usage():
    print >>sys.stderr, "Usage: shaback backup path"
    print >>sys.stderr, "       shaback fsck"
    print >>sys.stderr, "       shaback gc"
    print >>sys.stderr, "       shaback refresh"
    print >>sys.stderr, "       shaback restore path"
    print >>sys.stderr, "       shaback verify"
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
        elif sys.argv[a] == "--passphrase":
            a += 1
            Config.Passphrase = sys.argv[a]
        elif sys.argv[a] == "--verbose":
            Config.Verbose = True
        elif sys.argv[a] == "--exclude":
            a += 1
            Config.Exclude.append(sys.argv[a])
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
monitor = s3lib.Monitor()
s3.addmonitor(monitor)

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
elif command == "refresh":
    if len(args) == 0:
        refresh()
    else:
        usage()
elif command == "restore":
    if len(args) == 1:
        restore(args[0])
    else:
        usage()
elif command == "verify":
    if len(args) == 0:
        verify()
    else:
        usage()
else:
    usage()

print monitor._request
print monitor._bytesin
print s3lib.cost(monitor)
