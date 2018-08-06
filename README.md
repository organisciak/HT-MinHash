HathiTrust MinHash
--------------------

MinHash is a technique for estimating similarity of two sets quickly. Where a regular hash can fingerprint completely identical objects, MinHash is particularly useful for comparing sets that are similar but inexactly. It's used often for texts, where the set is comprised of the words in the text.

MinHash scales linearly, making it useful for an initial scan of similar texts before using a more computationally intensive algorithm.

This project uses datasketch to convert HTRC Extracted Features files to MinHashes, using a basic serialization method to write the HathiTrust volume ID and hash to file.

See [MinHash.ipynb](MinHash.ipynb) for details.