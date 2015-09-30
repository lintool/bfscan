package io.bfscan;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

import io.bfscan.data.*;
import io.bfscan.query.*;

public class ComRankerThread implements Runnable {
  public Thread Thr;
  private String threadName;
  private int start, end;
  private ComKeyValue[] data;
  private Query query;
  private int numTopDoc;
  private int qlen;
  private int thid;
  private Score [] allScore;
  private int[] tf = new int[10];
  private final MTPForDocVector doc = new MTPForDocVector();
  private int[] qtid = new int[100];
  private double[] idf =  new double[100];
  
  public ComRankerThread(String name, int start, int end, ComKeyValue[] data, Query query, int numTopDoc, int thid, Score [] allScore){
      threadName = name;
      this.start = start;
      this.end = end;
      this.data = data;
      this.numTopDoc = numTopDoc;
      this.query = query;
      this.thid = thid;
      this.allScore = allScore;
      qlen = query.TermID.size();
      for(int i = 0; i < qlen; i++) {
        idf[i] = query.idf.get(i);
        qtid[i] = query.TermID.get(i);
      }
  }
  
  public void run() {
    PriorityQueue<Score> scoreQueue = new PriorityQueue<Score>(numTopDoc, new Comparator<Score>() {
      public int compare(Score a, Score b) {
         if(a.score < b.score)
           return -1;
         else
           return 1;
      }
    });
    
    if(numTopDoc > 200)
      numTopDoc = 200;
    int n = 0;
    int dlen = 0;
    float score = 0.0f;
    float k1 = 1.0f;
    float b = 0.5f;
    float adl = 450.0f;
     for(int i = start; i < end; i++) {
       Arrays.fill(tf, 0);
       doc.fromIntArrayWritable(data[i].doc, doc);
       dlen = doc.getLength();
       if(qlen == 1) {
         score = 0.0f;
         int tf0 = 0;
           for (int termid : doc.getTermIds()) {
             if(termid == qtid[0]) tf0++; 
           }
           if(tf0 > 0) score += idf[0] * ((k1+1.0f) * tf0)/(k1*(1.0f-b+b*dlen/adl)+tf0);
         }
         
         else if(qlen == 2) {
           score = 0.0f;
           int tf0 = 0, tf1 = 0;
             for (int termid : doc.getTermIds()) {
               if(termid == qtid[0]) tf0++; 
               else if(termid == qtid[1]) tf1++;
             }
             if(tf0 > 0) score += idf[0] * ((k1+1.0f) * tf0)/(k1*(1.0f-b+b*dlen/adl)+tf0);
             if(tf1 > 0) score += idf[1] * ((k1+1.0f) * tf1)/(k1*(1.0f-b+b*dlen/adl)+tf1);
         }
         
         else if(qlen == 3) {
             score = 0.0f;
             int tf0 = 0, tf1 = 0, tf2 = 0;
               for (int termid : doc.getTermIds()) {
                 if(termid == qtid[0]) tf0++; 
                 else if(termid == qtid[1]) tf1++;
                 else if(termid == qtid[2]) tf2++;
               }
               if(tf0 > 0) score += idf[0] * ((k1+1.0f) * tf0)/(k1*(1.0f-b+b*dlen/adl)+tf0);
               if(tf1 > 0) score += idf[1] * ((k1+1.0f) * tf1)/(k1*(1.0f-b+b*dlen/adl)+tf1);
               if(tf2 > 0) score += idf[2] * ((k1+1.0f) * tf2)/(k1*(1.0f-b+b*dlen/adl)+tf2);
         }
         
         else if(qlen == 4) {
           score = 0.0f;
           int tf0 = 0, tf1 = 0, tf2 = 0, tf3 = 0;
             for (int termid : doc.getTermIds()) {
               if(termid == qtid[0]) tf0++; 
               else if(termid == qtid[1]) tf1++;
               else if(termid == qtid[2]) tf2++;
               else if(termid == qtid[3]) tf3++;
             }
             if(tf0 > 0) score += idf[0] * ((k1+1.0f) * tf0)/(k1*(1.0f-b+b*dlen/adl)+tf0);
             if(tf1 > 0) score += idf[1] * ((k1+1.0f) * tf1)/(k1*(1.0f-b+b*dlen/adl)+tf1);
             if(tf2 > 0) score += idf[2] * ((k1+1.0f) * tf2)/(k1*(1.0f-b+b*dlen/adl)+tf2);
             if(tf3 > 0) score += idf[3] * ((k1+1.0f) * tf3)/(k1*(1.0f-b+b*dlen/adl)+tf3);
        }
         
        else if(qlen == 5) {
             score = 0.0f;
             int tf0 = 0, tf1 = 0, tf2 = 0, tf3 = 0, tf4 = 0;
               for (int termid : doc.getTermIds()) {
                 if(termid == qtid[0]) tf0++; 
                 else if(termid == qtid[1]) tf1++;
                 else if(termid == qtid[2]) tf2++;
                 else if(termid == qtid[3]) tf3++;
                 else if(termid == qtid[4]) tf4++;
               }
               if(tf0 > 0) score += idf[0] * ((k1+1.0f) * tf0)/(k1*(1.0f-b+b*dlen/adl)+tf0);
               if(tf1 > 0) score += idf[1] * ((k1+1.0f) * tf1)/(k1*(1.0f-b+b*dlen/adl)+tf1);
               if(tf2 > 0) score += idf[2] * ((k1+1.0f) * tf2)/(k1*(1.0f-b+b*dlen/adl)+tf2);
               if(tf3 > 0) score += idf[3] * ((k1+1.0f) * tf3)/(k1*(1.0f-b+b*dlen/adl)+tf3);
               if(tf4 > 0) score += idf[4] * ((k1+1.0f) * tf4)/(k1*(1.0f-b+b*dlen/adl)+tf4);
         }
         
        else {
          Arrays.fill(tf, 0);
          score = 0.0f;
          for (int termid : doc.getTermIds()) {
            for(int k = 0; k < qlen; k++) {
              if(termid == qtid[k])
                tf[k]++;
            }
          }
          for(int k = 0; k < qlen; k++) {
            if(tf[k] > 0)
                 score += idf[k] * ((k1+1.0f) * tf[k])/(k1*(1.0f-b+b*dlen/adl)+tf[k]);
          }
        }
         
         if(score <= 0.0f)
           continue;
       
       
       if(n < numTopDoc) {
         scoreQueue.add(new Score(data[i].key, score, query.qno));
         n++;
       }
       else {
         if(scoreQueue.peek().score < score) {
           scoreQueue.poll();
           scoreQueue.add(new Score(data[i].key, score, query.qno));
         }
       }
     }
     // keep top k results
     int scoreQSize = Math.min(n, scoreQueue.size());
     int spos = numTopDoc * thid;
     for(int k = 0; k < scoreQSize; k++) {
       Score temp = scoreQueue.poll();
       allScore[spos] = new Score(temp.docid, temp.score, temp.qid);
       spos++;
     }
  }
  
  public void start ()
  {
     if (Thr == null)
     {
        Thr = new Thread (this, threadName);
        Thr.start ();
     }
  }
  
}
