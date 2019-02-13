/**
 * by Yuanbo Guo
 * Semantic Web and Agent Technology Lab, CSE Department, Lehigh University, USA
 * Copyright (C) 2004
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 59 Temple
 * Place, Suite 330, Boston, MA 02111-1307 USA
 */

package edu.lehigh.swat.bench.ubt;

import java.util.*;
import java.io.*;
import java.text.*;
import edu.lehigh.swat.bench.ubt.api.*;

public class Test extends RepositoryCreator {
  /** execution times of each query */
  public static final int QUERY_TIME = 10;
  /** name of file to hold the query test result */
  private static final String QUERY_TEST_RESULT_FILE = "result.txt";

  /** list of target systems */
  private Vector kbList_; //KbSpec
  /** list of test queries */
  private Vector queryList_; //QuerySpec
  /** result holder of query test */
  private QueryTestResult[][] queryTestResults_;
  /** Output stream for query test results. The file is created simply to ease the
   * manipulation of the results lateron, e.g., copying it to a Excel sheet.
   */
  private PrintStream queryTestResultFile_;
  /** flag indicating whether the target system is memory-based or not */
  private boolean isMemory_ = false;
  /** the current repository object */
  private Repository repository_ = null;

  /**
   * main method
   */
  public static void main (String[] args) {
    String arg = "";

    try {
      if (args.length < 1) throw new Exception();
      arg = args[0];
      if (arg.equals("query")) {
        if (args.length < 3) throw new Exception();
      }
      else if (arg.equals("load")) {
        if (args.length < 2) throw new Exception();
      }
      else if (arg.equals("memory")) {
        if (args.length < 3) throw new Exception();
      }
      else throw new Exception();
    }
    catch (Exception e) {
      System.err.println("Usage: Test load <kb config file>");
      System.err.println("    or Test query <kb config file> <query config file>");
      System.err.println("    or Test memory <kb config file> <query config file>");
      System.exit(-1);
    }

      Test test = new Test();
      if (arg.equals("query")) {
        test.testQuery(args[1], args[2]);
      }
      else if (arg.equals("load")) {
        test.testLoad(args[1]);
      }
      else if (arg.equals("memory")) {
        test.testMemory(args[1], args[2]);
      }

    System.exit(0);
  }

  /**
   * constructor.
   */
  public Test() {
  }

  /**
   * Starts the loading test defined in the specified config file.
   * @param kbConfigFile The knowledge base config file describing the target systems
   * and test data.
   */
  public void testLoad(String kbConfigFile) {
    createKbList(kbConfigFile);
    doTestLoading();
  }

  /**
   * Starts the query test defined in the specified config files.
   * @param kbConfigFile The knowledge base config file describing the target systems.
   * @param queryConfigFile The query config file describing the test queries.
   */
  public void testQuery(String kbConfigFile, String queryConfigFile) {
    try {
      queryTestResultFile_ = new PrintStream(new FileOutputStream(QUERY_TEST_RESULT_FILE));
      createKbList(kbConfigFile);
      createQueryList(queryConfigFile);
      doTestQuery();
      queryTestResultFile_.close();
    }
    catch (IOException e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * Starts the test on a memory-based system. Query test is conducted immediately
   * after loading.
   * NOTE: The current implementation is only workable in the case where there is
   * just one single system in the test, i.e., only one system is defined in the
   * config file.
   * @param kbConfigFile The knowledge base config file describing the target systems.
   * @param queryFile The query config file describing the test queries.
   */
  public void testMemory(String kbConfigFile, String queryFile) {
    isMemory_ = true;
    testLoad(kbConfigFile);
    testQuery(kbConfigFile, queryFile);
  }

  /**
   * Creates the target system list from the config file.
   * @param kbConfigFile The knowledge base config file.
   */
  private void createKbList(String kbConfigFile) {
    try {
      kbList_ = new KbConfigParser().createKbList(kbConfigFile, isMemory_);
    }
    catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /** Creates the query list from the config file
   * @param queryConfigFile The query config file.
   */
  private void createQueryList(String queryConfigFile) {
    try {
      queryList_ = new QueryConfigParser().createQueryList(queryConfigFile);
    }
    catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * Conducts the loading test.
   */
  private void doTestLoading() {
    KbSpecification kb;
    Date startTime, endTime;

    for (int i = 0; i < kbList_.size(); i++) {
      kb = (KbSpecification)kbList_.get(i);

      repository_ = createRepository(kb.kbClass_);
      if (repository_ == null) {
        System.err.println(kb.kbClass_ + ": class not found!");
        System.exit(-1);
      }

      repository_.setOntology(kb.ontology_);

      System.out.println();
      System.out.println("Started loading " + kb.id_);

      startTime = new Date();
      if (isMemory_) repository_.open(null);
      else repository_.open(kb.dbFile_);
      if (!repository_.load(kb.dataDir_)) {
        repository_.close();
        return;
      }
      if (!isMemory_) repository_.close();

      endTime = new Date();
      kb.duration = (endTime.getTime() - startTime.getTime())/1000;
      kbList_.set(i, kb);
      System.out.println();
      System.out.println();
      System.out.println("Finished loading " + kb.id_ + "\t" + kb.duration + " seconds");
    }

    showTestLoadingResults();
  }

  /** Conducts query test */
  private void doTestQuery() {
    queryTestResults_ = new QueryTestResult[kbList_.size()][queryList_.size()];
    for (int i = 0; i < kbList_.size(); i++) {
      testQueryOneKb(i);
    }
    showTestQueryResults();
  }

  /**
   * Conducts query test on the specified system.
   * @param index Index of the system in the target system list.
   */
  private void testQueryOneKb(int index) {
    QuerySpecification query;
    Date startTime, endTime;
    long duration = 0l, sum;
    KbSpecification kb;
    long resultNum = 0;
    QueryResult result;

    kb = (KbSpecification)kbList_.get(index);

    if (!isMemory_) repository_ = createRepository(kb.kbClass_);
    if (repository_ == null) {
      System.err.println(kb.kbClass_ + ": class not found!");
      System.exit(-1);
    }

    //set ontology
    repository_.setOntology(kb.ontology_);

    System.out.println();
    System.out.println("### Started testing " + kb.id_ + " ###");
    queryTestResultFile_.println(kb.id_);

    //test each query on this repository
    for (int i = 0; i < queryList_.size(); i++) {
      //open repository
      if (!isMemory_) repository_.open(kb.dbFile_);
      sum = 0l;
      query = (QuerySpecification)queryList_.get(i);
      System.out.println();
      System.out.println("~~~" + query.id_ + "~~~");
      //issue the query for QUERY_TIME times
      int j;
      for (j = 0; j < QUERY_TIME; j++) {
        startTime = new Date();
        result = repository_.issueQuery(query.query_);
        if (result == null) {
          repository_.close();
          System.err.println("Query error!");
          System.exit(-1);
        }
        //traverse the result set.
        resultNum = 0;
        while (result.next() != false) resultNum++;
        endTime = new Date();
        duration = endTime.getTime() - startTime.getTime();
        sum += duration;
        System.out.println("\tDuration: " + duration + "\tResult#: " + resultNum);
      } //end of for j
      //close the repository
      if (!isMemory_) repository_.close();
      //record the result
      queryTestResults_[index][i] = new QueryTestResult();
      queryTestResults_[index][i].duration_ = sum/j;
      queryTestResults_[index][i].resultNum_ = resultNum;
      result = null;
      queryTestResultFile_.println(sum/j);
      queryTestResultFile_.println(resultNum);
      queryTestResultFile_.println();
    } //end of for i
    System.out.println("### Finished testing " + kb.id_ + " ###");
  }

  /**
   * Displays the loading test results.
   */
  private void showTestLoadingResults() {
    KbSpecification kb;

    System.out.println();
    for (int i = 0; i < kbList_.size(); i++) {
      kb = (KbSpecification)kbList_.get(i);
      System.out.println("\t" + kb.id_ + "\t" + kb.duration + " seconds");
    }
  }

  /**
   * Displays the query test results.
   */
  private void showTestQueryResults() {
    KbSpecification kb;
    QuerySpecification query;
    QueryTestResult result;

    System.out.println();
    for (int i = 0; i < kbList_.size(); i++) {
      kb = (KbSpecification)kbList_.get(i);
      System.out.print("\t\t" + kb.id_);
    }
    System.out.println();
    System.out.println("\t\tTime/Result#");
    for (int j = 0; j < queryList_.size(); j++) {
      query = (QuerySpecification)queryList_.get(j);
      System.out.print(query.id_);
      for (int i = 0; i < kbList_.size(); i++) {
        kb = (KbSpecification)kbList_.get(i);
        result = queryTestResults_[i][j];
        System.out.print("\t\t" + result.duration_ + "/" + result.resultNum_);
      }
      System.out.println();
    }
  }
}