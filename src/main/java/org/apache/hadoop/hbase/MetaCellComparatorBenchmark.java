/**
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
package org.apache.hadoop.hbase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class MetaCellComparatorBenchmark {

  @State(Scope.Benchmark)
  public static class ByteBufferKeyValues {

    private Cell[] cells;

    @Setup
    public void setUp() throws IOException {
      // make the output stable
      Random rand = new Random(12345);
      cells = new Cell[1000];
      for (int i = 0; i < cells.length; i++) {
        RegionInfo regionInfo =
          RegionInfoBuilder.newBuilder(TableName.valueOf("test-table-" + rand.nextInt())).build();
        ExtendedCell cell = ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
          .setRow(regionInfo.getRegionName()).setFamily(HConstants.CATALOG_FAMILY)
          .setQualifier(HConstants.REGIONINFO_QUALIFIER).setTimestamp(System.currentTimeMillis())
          .setType(Type.Put).setValue(RegionInfo.toByteArray(regionInfo)).build();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        cell.write(out, false);
        byte[] bytes = out.toByteArray();
        ByteBuffer bb = ByteBuffer.allocateDirect(bytes.length);
        bb.put(bytes);
        bb.flip();
        cells[i] = new ByteBufferKeyValue(bb, 0, bytes.length);
      }
    }
  }

  @Benchmark
  public int compare(ByteBufferKeyValues kvs) {
    int sum = 0;
    for (int i = 0, n = kvs.cells.length - 1; i < n; i++) {
      sum += MetaCellComparator.META_COMPARATOR.compare(kvs.cells[i], kvs.cells[i + 1]);
    }
    return sum;
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
      .include(".*" + MetaCellComparatorBenchmark.class.getSimpleName() + ".*").build();

    new Runner(opt).run();
  }
}
