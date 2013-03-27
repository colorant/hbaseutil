package org.apache.hadoop.hbase;
/*
 * Copyright 2009 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.PerformanceEvaluation2.RunType;
import org.apache.hadoop.hbase.PerformanceEvaluation2.Test;
import org.apache.hadoop.hbase.regionserver.ColumnTracker;
import org.apache.hadoop.hbase.regionserver.ExplicitColumnTracker;
import org.apache.hadoop.hbase.regionserver.ScanQueryMatcher.MatchCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;

public class ColumnTrackerEval {
  public void test1(int times, int colnum) throws IOException{
    int maxVersions = 1;
    TreeSet<byte[]> columns = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < colnum; i++) {
      columns.add(Bytes.toBytes("col"+i));
    }

    ColumnTracker explicit = new ExplicitColumnTracker(columns, 0, maxVersions,
        Long.MIN_VALUE);

    long startTime = System.currentTimeMillis();
    
    for (int n = 0; n < times; n++) {
      explicit.reset();
      for (int i = 0; i < colnum; i++) {
        byte [] col = Bytes.toBytes("col"+i);
        explicit.checkColumn(col, 0, col.length, 1, KeyValue.Type.Put.getCode(),
            false);
      }
    }

    long elapsedTime = System.currentTimeMillis() - startTime;
    System.out.println("Finished in " + elapsedTime + "ms");
  }

  public static void main(final String[] args) throws IOException {
    int n = 0;
    int col = 0;
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      
      final String looptimes = "--times=";
      if (cmd.startsWith(looptimes)) {
        n = Integer.parseInt(cmd.substring(looptimes.length()));
        continue;
      }
      
      final String cols = "--cols=";
      if (cmd.startsWith(cols)) {
        col = Integer.parseInt(cmd.substring(cols.length()));
        continue;
      }
    }
    System.out.println("Start to loop " + n + " times : cols = " + col);
    ColumnTrackerEval ct = new ColumnTrackerEval();
    ct.test1(n, col);
  }
}
