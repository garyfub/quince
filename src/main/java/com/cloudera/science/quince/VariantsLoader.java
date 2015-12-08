/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.science.quince;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

/*
 * Loads variants from input files (which may be VCF, Avro, or Parquet), converts
 * to an appropriate model (which may be ADAM or GA4GH) determined by the subclass,
 * then partitions by key (contig, pos, sample_group).
 */
public abstract class VariantsLoader {

  /**
   * Load variants and extract a key.
   * key = (contig, pos, sample_group); value = Variant/Call Avro object
   * @param inputFormat the format of the input data (VCF, AVRO, or PARQUET)
   * @param inputPath the input data path
   * @param conf the Hadoop configuration
   * @param context the Spark context
   * @param variantsOnly whether to ignore samples and only load variants
   * @param flatten whether to flatten the data types
   * @param sampleGroup an identifier for the group of samples being loaded
   * @param samples the samples to include
   * @return the keyed variant or call records
   * @throws IOException if an I/O error is encountered during loading
   */
  protected abstract JavaPairRDD<Tuple3<String, Long, String>, SpecificRecord>
      loadKeyedRecords(String inputFormat, Path inputPath, Configuration conf,
          JavaSparkContext context, boolean variantsOnly, boolean flatten, String sampleGroup,
          Set<String> samples)
      throws IOException;

  protected abstract Class getSpecificRecordType(boolean variantsOnly, boolean flatten);

  /**
   * Expand gVCF blocks into a record per site.
   * @param records sorted keyed variants
   * @param segmentSize the number of base pairs in each segment partition
   * @return sorted keyed variants with one variant per site
   */
  protected JavaPairRDD<Tuple3<String, Long, String>, SpecificRecord>
    expandGvcfBlocks(JavaPairRDD<Tuple3<String, Long, String>, SpecificRecord> records,
      long segmentSize) {
    return records; // do nothing by default
  }

  /**
   * Load and partition variants.
   * key = (contig, pos, sample_group); value = Variant/Call Avro object
   * @param inputFormat the format of the input data (VCF, AVRO, or PARQUET)
   * @param inputPath the input data path
   * @param conf the Hadoop configuration
   * @param context the Spark context
   * @param variantsOnly whether to ignore samples and only load variants
   * @param flatten whether to flatten the data types
   * @param sampleGroup an identifier for the group of samples being loaded
   * @param samples the samples to include
   * @param redistribute whether to repartition the data by locus/sample group
   * @param segmentSize the number of base pairs in each segment partition
   * @param expandGvcfBlocks whether to expand gVCF blocks into a record per site
   * @param numReducers the number of reducers to use
   * @return the keyed variant or call records
   * @throws IOException if an I/O error is encountered during loading
   */
  public JavaRDD<GenericRecord> loadPartitionedVariants(
      String inputFormat, Path inputPath, Configuration conf,
      JavaSparkContext context, boolean variantsOnly, boolean flatten, String sampleGroup,
      Set<String> samples, boolean redistribute, long segmentSize,
      boolean expandGvcfBlocks, int numReducers)
      throws IOException {
    JavaPairRDD<Tuple3<String, Long, String>, SpecificRecord> locusSampleKeyedRecords =
        loadKeyedRecords(inputFormat, inputPath, conf, context, variantsOnly, flatten,
            sampleGroup, samples);

    // execute a DISTRIBUTE BY operation if requested
    JavaPairRDD<Tuple3<String, Long, String>, SpecificRecord> sortedRecords;
    if (redistribute) {
      Comparator<Tuple3<String, Long, String>> comparator =
          new DistributeComparator(segmentSize);
      sortedRecords = numReducers > 0 ?
          locusSampleKeyedRecords.sortByKey(comparator, true, numReducers) :
          locusSampleKeyedRecords.sortByKey(comparator);
    } else {
      // input data assumed to be already globally sorted
      sortedRecords = locusSampleKeyedRecords;
    }

    if (expandGvcfBlocks) {
      sortedRecords = expandGvcfBlocks(sortedRecords, segmentSize);
    }

    // add the partition columns - note that we add fields to the avro record
    // converting a Specific record to a Generic record (c.f. Crunch, where the partition
    // keys were separate from the record).
    return sortedRecords.map(new PartitionFn(segmentSize, sampleGroup));
  }

  public static final class DistributeComparator implements
      Comparator<Tuple3<String, Long, String>>, Serializable {
    private long segmentSize;

    public DistributeComparator(long segmentSize) {
      this.segmentSize = segmentSize;
    }
    @Override
    public int compare(Tuple3<String, Long, String> o1, Tuple3<String, Long, String> o2) {

      // Compare by (chr, chrSeg, sampleGroup) then by pos
      String chr1 = o1._1();
      long pos1 = o1._2();
      String sampleGroup1 = o1._3();
      long segment1 = getRangeStart(segmentSize, pos1);

      String chr2 = o2._1();
      long pos2 = o2._2();
      String sampleGroup2 = o2._3();
      long segment2 = getRangeStart(segmentSize, pos2);

      int cmp = chr1.compareTo(chr2);
      if (cmp != 0) {
        return cmp;
      }
      cmp = Long.compare(segment1, segment2);
      if (cmp != 0) {
        return cmp;
      }
      cmp = sampleGroup1.compareTo(sampleGroup2);
      if (cmp != 0) {
        return cmp;
      }

      return Long.compare(pos1, pos2);
    }
  }

  /**
   * Function to map a record key to a Hive-compatible string representing the
   * partition key.
   */
  public static final class LocusSampleToPartitionFn
      implements PairFunction<Tuple2<Tuple3<String, Long, String>, SpecificRecord>,
              String, SpecificRecord> {

    private long segmentSize;
    private String sampleGroup;

    public LocusSampleToPartitionFn(long segmentSize, String sampleGroup) {
      this.segmentSize = segmentSize;
      this.sampleGroup = sampleGroup;
    }

    @Override
    public Tuple2<String, SpecificRecord> call(Tuple2<Tuple3<String, Long, String>,
        SpecificRecord> pair) {
      Tuple3<String, Long, String> input = pair._1();
      StringBuilder sb = new StringBuilder();
      sb.append("chr=").append(input._1());
      sb.append("/pos=").append(getRangeStart(segmentSize, input._2()));
      if (sampleGroup != null) {
        sb.append("/sample_group=").append(sampleGroup);
      }
      return new Tuple2<>(sb.toString(), pair._2());
    }
  }

  /**
   * Function to add partition key fields (chr, pos, sample group) to an Avro record.
   * The record is changed to a GenericRecord in the process, since we don't have
   * generated classes (Specific) for any the types (Variant etc) with the partition key
   * fields.
   */
  public static final class PartitionFn
      implements Function<Tuple2<Tuple3<String, Long, String>, SpecificRecord>,
            GenericRecord> {

    private long segmentSize;
    private String sampleGroup;

    public PartitionFn(long segmentSize, String sampleGroup) {
      this.segmentSize = segmentSize;
      this.sampleGroup = sampleGroup;
    }

    @Override
    public GenericRecord call(Tuple2<Tuple3<String, Long, String>,
        SpecificRecord> pair) {
      Tuple3<String, Long, String> input = pair._1();
      SpecificRecord record = pair._2();
      Schema newSchema = AvroUtils.addPartitionColumns(record.getSchema(),
          sampleGroup != null);
      String chr = input._1();
      long pos = getRangeStart(segmentSize, input._2());
      return AvroUtils.addPartitionColumns(record, newSchema, chr, pos, sampleGroup);
    }
  }

  /**
   * @param size the size of the range
   * @param value the value that falls in a range
   * @return the start of the start (a multiple of <code>size</code>) that
   * <code>value</code> is in
   */
  public static long getRangeStart(long size, long value) {
    return Math.round(Math.floor(value / ((double) size))) * size;
  }
}
