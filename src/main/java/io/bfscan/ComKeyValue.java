package io.bfscan;

import tl.lin.data.array.IntArrayWritable;

public class ComKeyValue {
  public String key;
  public IntArrayWritable doc = new IntArrayWritable();
  
  public ComKeyValue(String k, IntArrayWritable v)
  {
    key = k;
    doc.setArray(v.getClone());
  }
  
}