package io.bfscan

// Needed for all Spark jobs.
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

// Needed for BFScan only
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.util.ArrayList
import java.util.Arrays
import java.util.Collection
import java.util.Collections
import java.util.Comparator
import java.util.List
import java.util.Comparator
import org.apache.hadoop.io.Text
import org.apache.spark.storage.StorageLevel
import tl.lin.data.array.IntArrayWritable
import io.bfscan._
import io.bfscan.query._
import io.bfscan.clueweb12.BuildDictionary
import io.bfscan.data.MTPForDocVector
import io.bfscan.data.TermStatistics
import io.bfscan.dictionary._

object ComBFScanSparkLocal {
  def main(args: Array[String]) {
  
        if (args.length < 5) {
      		System.err.println("Usage: <document vectors> <dictionary> <query file> <# top doc> <# thread>")
     		System.exit(1)
   	}
    
        // set paths 
        val docvectorPath = "file://" + args(0)
        val dictionaryPath = args(1)
        val queryFile = args(2)
        val topDoc = args(3).toInt
        val master = "local[" + args(4) + "]"
        
	// Set up the Spark configuration with our app name and any other config
	// parameters you want
	val sparkConf = new SparkConf().setMaster(master).setAppName("ComBFScanSparkLocal")

	// Use the config to create a spark context 
	val sc = new SparkContext(sparkConf)

	// read dictionary and term stat
	val conf = new Configuration()
        val fs = FileSystem.getLocal(conf) 
        val dictionary = new DefaultFrequencySortedDictionary(dictionaryPath, fs)
	val stats = new TermStatistics(new Path(dictionaryPath), fs)
        
	// read compressed document vectors and persist in memory
	var file = sc.sequenceFile(docvectorPath, classOf[Text], classOf[IntArrayWritable]).map{case (x, y) => (x.toString, y)}
	
	// IMPORTANT: To avoid same reference when reading hadoop sequence file, create new objects and map it to create new rdd
	var data = file.map(t=> {
		var y = t._2.getClone()
		var v = new IntArrayWritable(y)
		(t._1, v)
	})
	data.persist(StorageLevel.MEMORY_ONLY)

	// count the number of records
	val cs = data.count
	
	val numDoc = cs.toInt
	
	// read queries
	val allQuery = new getQueryTermIdStat(queryFile, dictionary, stats, numDoc);
        
        // read queries and query term stats
        var t = new Array[Int](1)
	t(0) = allQuery.nq
	var nq = t(0)
	var qno = new Array[Int](nq)
	var ql = new Array[Int](nq)
	var ids = new Array[Int](10*nq)
	var idfs = new Array[Float](10*nq)
	var qlsum = new Array[Int](nq)
	var c = 0
	for(m <- 0 to (nq-1)) 
	{
	  qlsum(m) = c
	  qno(m) = allQuery.query(m).qno
	  ql(m) = allQuery.query(m).TermID.size()
	  for(n <- 0 to (ql(m)-1)) {
	    ids(c) = allQuery.query(m).TermID.get(n)
	    idfs(c) = allQuery.query(m).idf.get(n)
	    c = c + 1
	  }
	}
	
	
	var result = new Array[String](1000*nq)
	var rc = 0
	val startTime = System.nanoTime();
	for(j <- 0 to (nq-1)) {
	var r = 1
	data.mapPartitions(thisdata=> {
		val currentDoc = new MTPForDocVector()
	        thisdata.map(t=> {
			currentDoc.fromIntArrayWritable(t._2, currentDoc)
			var terms = currentDoc.getTermIds()
			var doclen = terms.length
			var tf = new Array[Int](10)
			Arrays.fill(tf,0)
			val adl = 450.0
			for(i <- 0 to (doclen - 1)) {
			    for(k <- 0 to (ql(j)-1))
				if(terms(i) == ids(qlsum(j)+k)) { 
				  tf(k) = tf(k) + 1
				}
			}
			val k1 = 1.0
			val b = 0.5
			var tff = 0.0
			var score = 0.0
			// compute score 
			for(k <- 0 to (ql(j)-1)) {
			 tff = ((k1+1.0) * tf(k))/(k1 * (1.0-b+b*(doclen/adl)) + tf(k))
			 score += (tff * idfs(qlsum(j)+k))
			}
			(score, t._1, qno(j))
		}
		)
	}).top(topDoc).foreach(i => {var out = i._3.toString + " Q0 " + i._2 + " " + r.toString + " " + i._1.toString + " bm25-spark" 
		result(rc)=out
		r = r + 1
		rc = rc + 1})
	}
	
	val endTime = System.nanoTime();
	
	// print the rank lists
    	for(i <-0 to (rc-1)) 
    		println(result(i))
    	
    	println("took " + (endTime-startTime)/1000000000.0 + " second for " + nq + " queries")
  }
}
