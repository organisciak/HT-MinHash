import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")

from htrc_features import FeatureReader, utils
from hash_utils import make_hash
import pandas as pd
import os

def make_hashes(vocab, ids=None, paths=None, **kwargs):
    if ids and paths:
        raise "Can't include both ids and paths"
    elif ids:
        fr = FeatureReader(ids=ids)
    elif paths:
        fr = FeatureReader(paths=paths)
    else:
        raise "Need either a list of ids or paths"

    i = 0
    for vol in fr.volumes():
        tokens = set(vol.tokens()).intersection(vocab)
        i += 1
        if i % 100 == 0:
            print(os.getpid(), i, 'files processed')
        yield make_hash(vol.id, tokens, **kwargs)
        
def write_hashes(outfile, ids=None, paths=None, **kwargs):
    print(os.getpid(), 'Loading Vocab')
    vocab = set(pd.read_csv('../../datasets/token-frequencies/wordlist.tsv.gz', sep='\t',
                        compression='gzip', usecols=[0]).token.tolist())
    
    with open(outfile, mode='wb') as f:
        for hash_serial in make_hashes(vocab, ids=ids, paths=paths, **kwargs):
            f.write(hash_serial)
            
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Process a BW file into a Minhash and save using a succinct serialization format.')
    parser.add_argument('outdir', type=str, help='Directory to save Minhashes. File name is hashes.{RANDINT}.dat.')
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--ids', type=str, nargs='+', help='HathiTrust IDs to convert to MinHash. Mutually exclusive with --paths.')
    group.add_argument('--paths', type=str, nargs='+', help='Paths to HathiTrust EF file to convert to MinHash. Mutually exclusive with --ids.')    
    
    parser.add_argument('--num_perm', type=int, default=128, help='Number of permutations for MinHash. Defaults to 128')
    args = parser.parse_args()

    import numpy.random as random

    filename = "hashes.%d.dat" % random.randint(10000)
    outfile = os.path.join(args.outdir, filename)
    
    write_hashes(outfile=outfile, ids=args.ids, paths=args.paths, num_perm=args.num_perm)