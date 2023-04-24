package org.apache.hadoop.hbase;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.hbase.util.Bytes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@Fork(1)
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ComparatorBenchmark {

  @Param({"", "fam1"})
  String p1;

  @Param({"", "fam1"})
  String p2;

  byte[] row1 = Bytes.toBytes("row1");
  byte[] qual1 = Bytes.toBytes("qual1");
  byte[] val = Bytes.toBytes("val");
  KeyValue kv1;
  KeyValue kv2;
  Cell bbCell1;
  Cell bbCell2;

  @Setup
  public void initParams() {
    byte[] fam0 = Bytes.toBytes(p1);
    byte[] fam1 = Bytes.toBytes(p2);
    Pair<byte[], byte[]> famPair = new Pair<>(fam0, fam1);
    kv1 = new KeyValue(row1, famPair.getFirst(), qual1, val);
    kv2 = new KeyValue(row1, famPair.getSecond(), qual1, val);
    ByteBuffer buffer = ByteBuffer.wrap(kv1.getBuffer());
    bbCell1 = new ByteBufferKeyValue(buffer, 0, buffer.remaining());
    buffer = ByteBuffer.wrap(kv2.getBuffer());
    bbCell2 = new ByteBufferKeyValue(buffer, 0, buffer.remaining());
  }

  @Benchmark
  public void oldCompareKV() {
    CellComparatorImpl.COMPARATOR.compare(kv1, kv2);
  }

  @Benchmark
  public void oldCompareBBKV() {
    CellComparatorImpl.COMPARATOR.compare(bbCell1, bbCell2);
  }

  @Benchmark
  public void oldCompareKVVsBBKV() {
    CellComparatorImpl.COMPARATOR.compare(kv1, bbCell2);
  }

  @Benchmark
  public void newCompareKV() {
    CellComparatorNewImpl.COMPARATOR.compare(kv1, kv2);
  }

  @Benchmark
  public void newCompareBBKV() {
    CellComparatorNewImpl.COMPARATOR.compare(bbCell1, bbCell2);
  }

  @Benchmark
  public void newCompareKVVsBBKV() {
    CellComparatorNewImpl.COMPARATOR.compare(kv1, bbCell2);
  }

  @Benchmark
  public void innerStoreCompareKV() {
    InnerStoreCellComparator.INNER_STORE_COMPARATOR.compare(kv1, kv2);
  }

  @Benchmark
  public void innerStoreCompareBBKV() {
    InnerStoreCellComparator.INNER_STORE_COMPARATOR.compare(bbCell1, bbCell2);
  }

  @Benchmark
  public void innerStoreCompareKVVsBBKV() {
    InnerStoreCellComparator.INNER_STORE_COMPARATOR.compare(kv1, bbCell2);
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(ComparatorBenchmark.class.getSimpleName()).build();
    new Runner(opt).run();
  }
}
