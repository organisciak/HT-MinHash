import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")

'''
To parallize with GNU Parallel on the command line:

    find ../datasets/volume-tables/data/. -name '*h5' | parallel -j5 -n1 -u python MakeMinHash.py {} data/minhashes/{/.}.dat
    
'''
from hash_utils import make_hash

# Since the input files are already sorted, don't groupby on the entire input, just on the partition
def group_vols(df):
    return df.groupby('id', sort=False).token.apply(set) #.reset_index()

def join_sets(s):
    ''' If a groupby has more than wordset, merge it. This will only happen at the splits between
    partitions'''
    l = len(s)
    newset = s.iloc[0]
    for i in range(1,l):
        a = len(newset)
        newset = newset.union(s.iloc[i])
        if a > len(newset):
            print(a, l, len(newset), 'huh?')
    return newset

def make_hashes(infile, outfile, stop=None, num_perm=128):
    import os
    import time
    import pandas as pd
    import dask.dataframe as dd
    import dask
    from multiprocessing.pool import ThreadPool
    
    print(os.getpid(), ": Loading volume refs")
    volref = pd.read_csv('../datasets/volume-tables/volume-id-key.gz', sep='\t', compression='gzip',
                         names=['index', 'htid']).set_index('index').to_dict()['htid']
    print(os.getpid(), ": Loading term refs")
    termrefdf = dd.read_csv('../datasets/token-frequencies/wordlist.tsv.gz', sep='\t', 
                            compression='gzip', blocksize=None)
    termref = termrefdf.token.compute().to_dict()
    
    print(os.getpid(), ": Loading df")
    #
    # Q: Why am I using dask if barely making use of its multithreading or multiprocessing?
    # A: Memory. Dask seems to be more efficient at memory use and gc than pd.read_x by some
    #     magic that eludes me.
    ddf = dd.read_hdf(infile, 'unigrams', stop=stop, chunksize=100000, mode='r')[['token']].reset_index()
    #grouped_ddf = ddf.reset_index().groupby('id').token.apply(set, meta=('tokens', 'O'))
    grouped_ddf = ddf.map_partitions(group_vols, meta=('tokens', 'O')) #meta={'id':'i8', 'token': 'O'}) #.groupby('id').token.apply(join_sets, meta=('token', 'O')) #.reset_index()
    
    i = 0
    with open(outfile, mode='wb') as f:
        #with dask.config.set(pool=ThreadPool(16)):
        with dask.config.set(scheduler='single-threaded'):
            prev = None
            for current in grouped_ddf.iteritems():
                if not prev:
                    prev = current
                    continue
                if current[0] != prev[0]:
                    # Make the hash if we're onto a new volume
                    volid, row = prev
                    htid = str(volref[volid])
                    wordset = [termref[y] for y in row if y in termref]
                    h = make_hash(htid=htid, wordset=wordset, num_perm=num_perm)
                    f.write(h)
                    i += 1
                    if i % 1000 == 0:
                        print(os.getpid(), "%d volumes processed" % i)
                    prev = current
                else:
                    volid = current[0]
                    newset = prev[1].union(current[1])
                    prev = volid, newset
            
            # Finally
            htid = str(volref[prev[0]])
            wordset = [termref[y] for y in row if y in termref]
            h = make_hash(htid=htid, wordset=wordset, num_perm=num_perm)
            f.write(h)
            print(os.getpid(), "complete with", i+1, "volumes")
                          
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Process a BW file into a Minhash and save using a succinct serialization format.')
    parser.add_argument('input', type=str, help='Path to the input h5 file.')
    parser.add_argument('output', type=str, help='Location to save the output data file.')
    parser.add_argument('--num_perm', type=int, default=128, help='Number of permutations for MinHash. Defaults to 128')
    parser.add_argument('--nrows', type=int, default=None, help='Limit number of rows to read from input. Useful for testing.')
    args = parser.parse_args()

    make_hashes(args.input, args.output, stop=args.nrows, num_perm=args.num_perm)