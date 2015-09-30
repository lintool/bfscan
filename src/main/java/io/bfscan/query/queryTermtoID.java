package io.bfscan.query;

import org.apache.lucene.analysis.Analyzer;

import tl.lin.lucene.AnalyzerUtils;
import io.bfscan.dictionary.*;
import io.bfscan.util.AnalyzerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class queryTermtoID {
 public Query[] query = new Query[5000]; 
 public int nq;
 private static Analyzer ANALYZER = AnalyzerFactory.getAnalyzer("porter");
 
 public queryTermtoID(String fileName, DefaultFrequencySortedDictionary dictionary)
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
       for (String token : tokens) {
         int id = dictionary.getId(token);
         if (id != -1) {
           TermID.add(id);
         }
       }
       query[i] = new Query(qno, TermID);
       i++;  
     }
     nq = i; 
     in.close();
   } 
   catch (IOException e) {
   }
 }
  
}