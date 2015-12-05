Search Using Brute Force Scans
==============================

Hadoop tools for manipulating ClueWeb collections and performing document retrieval using brute force scan techniques.

Getting Stated
--------------

You can clone the repo with the following command:

```
$ git clone git://github.com/lintool/bfscan.git
``` 

Once you've cloned the repository, build the package with Maven:

```
$ mvn clean package appassembler:assemble
```

Two notes:

+ `appassembler:assemble` automatically generates a few launch scripts for you.
+ in addition to the normal jar (`bfscan-0.1-SNAPSHOT.jar`), this package uses the [Maven Shade plugin](http://maven.apache.org/plugins/maven-shade-plugin/) to create a "fat jar" (`bfscan-0.1-SNAPSHOT-fatjar.jar`) that includes all dependencies except for Hadoop, so that the jar can be directly submitted via `hadoop jar ...`.

To automatically generate project files for Eclipse:

```
$ mvn eclipse:clean
$ mvn eclipse:eclipse
```

You can then use Eclipse's Import "Existing Projects into Workspace" functionality to import the project.

Counting Records
----------------

For sanity checking and as a "template" for other Hadoop jobs, the package provides a simple program to count WARC records in ClueWeb12:

```
hadoop jar target/bfscan-0.1-SNAPSHOT-fatjar.jar io.bfscan.CountWarcRecords \
 -clueweb12 -input '/path/to/warc/files/'
```

Examples of `/path/to/warc/files/` are:

+ `/collections/ClueWeb12/Disk1/ClueWeb12_00/*/*.warc.gz`: for a single ClueWeb12 segment
+ `/collections/ClueWeb12/Disk1/ClueWeb12_*/*/*.warc.gz`: for an entire ClueWeb12 disk
+ `/collections/ClueWeb12/Disk[1234]/ClueWeb12_*/*/*.warc.gz`: for all of ClueWeb12

Building a Dictionary
---------------------

The next step is to build a dictionary that provides three capabilities:

+ a bidirectional mapping from terms (strings) to termids (integers)
+ lookup of document frequency (*df*) by term or termid
+ lookup of collection frequency (*cf*) by term or termid

To build the dictionary, we must first compute the term statistics. It's easier to compute term statistics disk by disk so that the Hadoop jobs are smaller and more manageable:

```
hadoop jar target/bfscan-0.1-SNAPSHOT-fatjar.jar io.bfscan.clueweb12.ComputeTermStatistics \
 -input '/collections/ClueWeb12/Disk1/ClueWeb12_*/*/*.warc.gz' -output cw12-term-stats/disk1 \
 -preprocessing porter

hadoop jar target/bfscan-0.1-SNAPSHOT-fatjar.jar io.bfscan.clueweb12.ComputeTermStatistics \
 -input '/collections/ClueWeb12/Disk2/ClueWeb12_*/*/*.warc.gz' -output cw12-term-stats/disk2 \
 -preprocessing porter

hadoop jar target/bfscan-0.1-SNAPSHOT-fatjar.jar io.bfscan.clueweb12.ComputeTermStatistics \
 -input '/collections/ClueWeb12/Disk3/ClueWeb12_*/*/*.warc.gz' -output cw12-term-stats/disk3 \
 -preprocessing porter

hadoop jar target/bfscan-0.1-SNAPSHOT-fatjar.jar io.bfscan.clueweb12.ComputeTermStatistics \
 -input '/collections/ClueWeb12/Disk4/ClueWeb12_*/*/*.warc.gz' -output cw12-term-stats/disk4 \
 -preprocessing porter
```

By default, the program throws away all terms with *df* less than 100, but this parameter can be set on the command line.

Next, merge all the term statistics together:

```
hadoop jar target/bfscan-0.1-SNAPSHOT-fatjar.jar io.bfscan.clueweb12.MergeTermStatistics \
 -input 'cw12-term-stats/disk*' -output cw12-term-stats-all
```

Finally, build the dictionary:

```
hadoop jar target/bfscan-0.1-SNAPSHOT-fatjar.jar io.bfscan.clueweb12.BuildDictionary \
 -input cw12-term-stats-all -output cw12-dictionary -count 9364999
```

Provide the number of terms in the dictionary via the `-count` option. That value is simply the number of reduce output records from `MergeTermStatistics`.

To explore the contents of the dictionary, use this interactive program:

```
hadoop jar target/bfscan-0.1-SNAPSHOT-fatjar.jar \
 io.bfscan.dictionary.DefaultFrequencySortedDictionary cw12-dictionary
```

**Implementation details:** Tokenization is performed by first using Jsoup throw away all markup information and then passing the resulting text through Lucene's `PorterAnalyzer`.

The dictionary has two components: the terms are stored as a front-coded list (which necessarily means that the terms must be sorted); a monotone minimal perfect hash function is used to hash terms (strings) into the lexicographic position. Term to termid lookup is accomplished by the hashing function (to avoid binary searching through the front-coded data structure, which is expensive). Termid to term lookup is accomplished by direct accesses into the front-coded list. An additional mapping table is used to convert the lexicographic position into the (*df*-sorted) termid. 

Building Document Vectors
-------------------------

With the dictionary, we can now convert the entire collection into a sequence of document vectors, where each document vector is represented by a sequence of termids; the termids map to the sequence of terms that comprise the document. These document vectors are much more compact and much faster to scan for processing purposes.

The document vector is represented by the interface `io.bfscan.data.DocVector`. Currently, there are two concrete implementations:

+ `VByteDocVector`, which uses Hadoop's built-in utilities for writing variable-length integers (what Hadoop calls VInt).
+ `PForDocVector`, which uses PFor compression from Daniel Lemire's [JavaFastPFOR](https://github.com/lemire/JavaFastPFOR/) package.

To build document vectors, use either `BuildVByteDocVectors` or `BuildPForDocVectors`:

```
hadoop jar target/bfscan-0.1-SNAPSHOT-fatjar.jar io.bfscan.clueweb12.BuildVByteDocVectors \
 -input '/collections/ClueWeb12/Disk1/ClueWeb12_*/*/*.warc.gz' \
 -output cw12-docvectors/vbyte/disk1 -reducers 100 \
 -dictionary cw12-dictionary -preprocessing porter
```

Once again, it's advisable to run on a disk at a time in order to keep the Hadoop job sizes manageable. Note that the program uses identity reducers to repartition the document vectors into 100 parts (to avoid the small files problem).

The output directory will contain `SequenceFile`s, with a `Text` containing the WARC-TREC-ID as the key. For VByte, the value will be a `BytesWritable` object; for PFor, the value will be an `IntArrayWritable` object.

To process these document vectors, either use `ProcessVByteDocVectors` or `ProcessPForDocVectors` in the `io.bfscan.clueweb12` package, which provides sample code for consuming these document vectors and converting the termids back into terms.

Size comparisons, on the entire ClueWeb12 collection:

+ 5.54 TB: original compressed WARC files
+ 867 GB: repackaged as `VByteDocVector`s
+ 665 GB: repackaged as `PForDocVector`s

For reference, there are 344 billion terms in the entire collection when processed in the above manner.


Brute-Force Scan Document Retrieval (Java)
-------------------------------------------------
There are four Java implementations of Brute-Force Scan based document retrieval depending upon different document representations. In all cases, BM25 retrieval model is used. Document vectors are compressed using `pfor` encoding scheme.

**How to run:**

**1. On uncompressed document as flat array of terms** 
```
java -cp target/bfscan-0.1-SNAPSHOT-fatjar.jar io.bfscan.BFScan \
<document vectors path> <# doc to return> <dictionary path> <# thread> <query file> <# doc in collection>

Example:
java -cp target/bfscan-0.1-SNAPSHOT-fatjar.jar io.bfscan.BFScan \
/scratch1/jiaul/bfscan/docvectors-cw09b 1000 /scratch1/jiaul/bfscan/dictionary-cw09b 40 /scratch1/jiaul/bfscan/cw09-topics 56000000
```

**2. On compressed document as flat array of terms** 
```
java -cp target/bfscan-0.1-SNAPSHOT-fatjar.jar io.bfscan.ComBFScan \
<document vectors path> <# doc to return> <dictionary path> <numThread> <query file> <# doc in collection>
```
**3. On uncompressed  document vector of unique terms (sorted) and their tfs**
```
java -cp target/bfscan-0.1-SNAPSHOT-fatjar.jar io.bfscan.UniqTermBFScan \
<document vectors path> <# doc to return> <dictionary path> <# thread> <query file> <# doc. in collection>
```
**4. On compressed  document vector of unique terms (sorted) and their tfs**
```
java -cp target/bfscan-0.1-SNAPSHOT-fatjar.jar io.bfscan.ComUniqTermBFScan \
<document vectors path> <# doc to return> <dictionary path> <# thread> <query file> <# doc. in collection>
```

**Format of the query file:** The query file contains all queries, one line for each query. The line starts with the query number (integer), followed by `colon`, followed by the query words.  Here is an example. 
```
15:espn sports
16:arizona game and fish
```



Brute-Force Scan Document Retrieval (Spark Cluster)
-------------------------------------------------
As in Java, there are four Spark implementations of Brute-Force Scan. All codes are written in scala.

**How to run:**

**1. On uncompressed document as flat array of terms**
```
spark-submit --class io.bfscan.BFScanSpark --num-executors 100 --executor-memory 2G \
target/bfscan-0.1-SNAPSHOT-fatjar.jar <document vectors path> <dictionary> <query file> <# doc to return>

Example:
spark-submit --class io.bfscan.BFScanSpark --num-executors 100 --executor-memory 2G target/bfscan-0.1-SNAPSHOT-fatjar.jar \
/user/jiaul/docvectors-cw09b /user/jiaul/dictionary-cw09b /user/jiaul/cw09.topics 1000
```
**2. On compressed document as flat array of terms**
```
spark-submit --class io.bfscan.ComBFScanSpark --num-executors 100 --executor-memory 2G \
target/bfscan-0.1-SNAPSHOT-fatjar.jar <document vectors path> <dictionary> <query file> <# doc to return>
```
**3. On uncompressed  document vector of unique terms (sorted) and their tfs**
```
spark-submit --class io.bfscan.UniqTermBFScanSpark --num-executors 100 --executor-memory 2G \
target/bfscan-0.1-SNAPSHOT-fatjar.jar <document vectors path> <dictionary> <query file> <# doc to return>
```
**4. On compressed  document vector of unique terms (sorted) and their tfs**
```
spark-submit --class io.bfscan.ComUniqTermBFScanSpark --num-executors 100 --executor-memory 2G \
target/bfscan-0.1-SNAPSHOT-fatjar.jar <document vectors path> <dictionary> <query file> <# doc to return>
```

Brute-Force Scan Document Retrieval (Spark Local)
-------------------------------------------------
In addition, the package also contains classes to run Spark in `local` mode, meaning instead of running multiple executors on different machines in the cluster, spark will run multiple threads on a local machine. The argument `<# thread>` specifies how many threads spark will use.

**How to run:**

**1. On uncompressed document as flat array of terms**
```
spark-submit --class io.bfscan.BFScanSparkLocal --driver-memory 100G \
target/bfscan-0.1-SNAPSHOT-fatjar.jar <document vectors path> <dictionary> <query file> <# doc to return> <# thread>

Example:
spark-submit --class io.bfscan.BFScanSparkLocal --driver-memory 100G target/bfscan-0.1-SNAPSHOT-fatjar.jar \
/scratch1/jiaul/bfscan/docvectors-cw09b /scratch1/jiaul/bfscan/dictionary-cw09b /scratch1/jiaul/bfscan/cw09-topics 1000 40
```
**2. On compressed document as flat array of terms**
```
spark-submit --class io.bfscan.ComBFScanSparkLocal --driver-memory 100G \
target/bfscan-0.1-SNAPSHOT-fatjar.jar <document vectors path> <dictionary> <query file> <# doc to return> <# thread>
```
**3. On uncompressed  document vector of unique terms (sorted) and their tfs**
```
spark-submit --class io.bfscan.UniqTermBFScanSparkLocal --num-executors 100 --driver-memory 100G \
target/bfscan-0.1-SNAPSHOT-fatjar.jar <document vectors path> <dictionary> <query file> <# doc to return> <# thread>
```
**4. On compressed  document vector of unique terms (sorted) and their tfs**
```
spark-submit --class io.bfscan.ComUniqTermBFScanSparkLocal --driver-memory 100G \
target/bfscan-0.1-SNAPSHOT-fatjar.jar <document vectors path> <dictionary> <query file> <# doc to return> <# thread>
```


License
-------

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
