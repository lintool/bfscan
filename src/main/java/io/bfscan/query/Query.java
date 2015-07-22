package io.bfscan.query;

import java.util.*;

public class Query {
  public int qno;
  public List<Integer> TermID;
  public List<Float> idf;
  public List<Long> ctf;
  public Query(int qno, List<Integer> TermID)
  {
    this.qno = qno;
    this.TermID = TermID;
  }
  
  public Query(int qno, List<Integer> TermID, List<Float> idf, List<Long> ctf)
  {
    this.qno = qno;
    this.TermID = TermID;
    this.idf = idf;
    this.ctf = ctf;
  }
  
  
}