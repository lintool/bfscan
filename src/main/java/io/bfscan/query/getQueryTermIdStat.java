package io.bfscan.query;

import org.clueweb.data.TermStatistics;
import org.clueweb.dictionary.*;
import org.apache.lucene.analysis.Analyzer;
import org.clueweb.util.AnalyzerFactory;

import tl.lin.lucene.AnalyzerUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class getQueryTermIdStat implements java.io.Serializable {
 public Query[] query = new Query[5000];
 public int nq;
 private static Analyzer ANALYZER = AnalyzerFactory.getAnalyzer("porter");
 
 public getQueryTermIdStat(String fileName, DefaultFrequencySortedDictionary dictionary, TermStatistics stats, int cs)
 {
   try {
     BufferedReader in = new BufferedReader(new FileReader(fileName));
     String str;
     int i = 0;
     int qno;
     while ((str = in.readLine()) != null) {
       // process here
       String[] parts = str.split(":");
       qno = Integer.parseInt(parts[0]);
       List<String> tokens = AnalyzerUtils.parse(ANALYZER, parts[1]);
       List<Integer> TermID = new ArrayList<Integer>();
       List<Float> idf = new ArrayList<Float>();
       List<Long> ctf = new ArrayList<Long>();
       for (String token : tokens) {
         int id = dictionary.getId(token);
         if (id != -1) {
           TermID.add(id);
           int df = stats.getDf(id);
           idf.add((float) Math.log((1.0f*cs-df+0.5f)/(df+0.5f)));
           ctf.add(stats.getCf(id));
         }
       }
       query[i] = new Query(qno, TermID, idf, ctf);
       
       i++;  
     }
     nq = i; 
     in.close();
   } 
   catch (IOException e) {
   }
 }
  
}