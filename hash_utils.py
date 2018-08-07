from __future__ import generator_stop
from datasketch import MinHash, LeanMinHash
import struct

STRUCT_FMT = "%sqi30s%dI"

def make_hash(htid, wordset, **kwargs):
    ''' Make a LeanMinHash and serialize as bytes '''
    m = MinHash(**kwargs)
    for wordid in wordset:
        m.update(str(wordid).encode('utf-8'))
    lm = LeanMinHash(m)
    return serialize_lm(htid,lm)

def serialize_lm(htid,lm):
    ''' Serialize LeanMinHash, using custom format that includes string name '''
    # Format
    ## seed (8byte long long), length of hash (4 byte),
    ## volume name (max 30 char string), hashes (4 bytes each, * len)
    fmt = STRUCT_FMT % ('@', len(lm))
    return struct.pack(fmt, lm.seed, len(lm), htid.encode('utf-8'), *lm.hashvalues)

def deserialize_lm(buf, length, encoding='utf-8', **kwargs):
    fmt = STRUCT_FMT % ('@', length)
    try:
        unpack = struct.unpack_from(fmt, buf, 0)
    except TypeError:
        unpack = struct.unpack_from(fmt, memoryview(buf), 0)
    seed, num_perm, volid = unpack[:3]
    hashvalues = unpack[3:]
    lmh = object.__new__(LeanMinHash)
    lmh._initialize_slots(seed, hashvalues)
    return volid.strip(b'\x00').decode(encoding), lmh

class HashReader(object):
    
    def __init__(self, filename):
        self.filename = filename

    def __enter__(self):
        self.fd = open(self.filename, mode='rb')
        # Get Length of the first hash, assuming that the file is solely
        # comprised of identical length hashes.
        peek = self.fd.peek(12)
        seed, self.num_perm = struct.unpack('@qi', peek[:12])
        fmt = "%sqi30s%dI" % ('@', self.num_perm) 
        self.size = struct.calcsize(fmt)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.fd.close()
        
    def hashes(self, batch_size=100, encoding='utf-8', deserialize=True, **kwargs):
        ''' Read hashes as an iterator. It doesn't make sense to read a few hundred bytes at a time,
        so the reader collects hashsize*batchsize bytes at a time.'''
        
        j = 0
        while True:
            data = self.fd.read(self.size*batch_size)
            if not data:
                # Once stream has been exhausted, will return zero bytes
                break

            for i in range(0, batch_size): 
                start, end = i*self.size, (i+1)*self.size
                if end > len(data):
                    break
                j += 1
                if deserialize:
                    yield deserialize_lm(data[start:end], self.num_perm, **kwargs)
                else:
                    part = data[start:end]
                    # Return full serialized hash, alongside extracted volume id
                    htid_dirty = struct.unpack('@qi30s', part[:42])[2]
                    htid = htid_dirty.strip(b'\x00').decode(encoding)
                    yield (htid, part)