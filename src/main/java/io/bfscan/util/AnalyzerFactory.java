package io.bfscan.util;

import io.bfscan.dictionary.PorterAnalyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.Version;

public class AnalyzerFactory {

  public static Analyzer getAnalyzer(String analyzerType) {
    if (analyzerType.equals("standard")) {
      return new org.apache.lucene.analysis.standard.StandardAnalyzer(Version.LUCENE_43);
    }

    if (analyzerType.equals("porter")) {
      return new PorterAnalyzer();
    }

    return null;
  }

  public static String getOptions() {
    return "standard|porter";
  }
}