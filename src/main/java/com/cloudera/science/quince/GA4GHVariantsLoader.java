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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.ga4gh.models.FlatVariantCall;
import org.ga4gh.models.Variant;
import org.opencb.hpg.bigdata.core.converters.variation.VariantContext2VariantConverter;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import scala.Tuple2;
import scala.Tuple3;

public class GA4GHVariantsLoader extends VariantsLoader {

  @Override
  protected JavaPairRDD<Tuple3<String, Long, String>, SpecificRecord>
    loadKeyedRecords(String inputFormat, Path inputPath, Configuration conf,
        JavaSparkContext context, boolean variantsOnly, boolean flatten, String sampleGroup,
        Set<String> samples)
        throws IOException {
    JavaRDD<Variant> variants = readVariants(inputFormat, inputPath,
        conf, context, sampleGroup);

    GA4GHToKeyedSpecificRecordFn converter =
        new GA4GHToKeyedSpecificRecordFn(variantsOnly, flatten, sampleGroup, samples);

    return variants.flatMapToPair(converter);
  }

  @Override
  protected Class getSpecificRecordType(boolean variantsOnly, boolean flatten) {
    return flatten ? FlatVariantCall.class : Variant.class;
  }

  /*
   * Read input files (which may be VCF, Avro, or Parquet) and return a PCollection
   * of GA4GH Variant objects.
   */
  @SuppressWarnings("unchecked")
  private static JavaRDD<Variant> readVariants(String inputFormat, Path inputPath,
      Configuration conf, JavaSparkContext context, String sampleGroup) throws IOException {
    JavaRDD<Variant> variants;
    if (inputFormat.equals("VCF")) {
      JavaPairRDD<LongWritable, VariantContextWritable>
          vcfRecords = context.newAPIHadoopFile(inputPath.toString(),
            VCFInputFormat.class, LongWritable.class, VariantContextWritable.class, conf);
      VariantContext2VariantConverter converter = VCFToGA4GHVariantFn.buildConverter(
          conf, FileUtils.findVcfs(inputPath, conf), sampleGroup);
      Broadcast<VariantContext2VariantConverter> converterBroadcast =
          context.broadcast(converter);
      variants = vcfRecords.map(new VCFToGA4GHVariantFn(converterBroadcast));
    } else if (inputFormat.equals("AVRO")) {
      variants = context.newAPIHadoopFile(inputPath.toString(), AvroKeyInputFormat.class,
          AvroKey.class, NullWritable.class, conf).keys()
          .map(new Function<AvroKey, Variant>() {
        @Override
        @SuppressWarnings("unchecked")
        public Variant call(AvroKey avroKey) throws Exception {
          return (Variant) avroKey.datum();
        }
      });
    } else if (inputFormat.equals("PARQUET")) {
      variants = context.newAPIHadoopFile(inputPath.toString(), AvroParquetInputFormat.class,
          Void.class, Variant.class, conf).values();
    } else {
      throw new IllegalStateException("Unrecognized input format: " + inputFormat);
    }
    return variants;
  }

  @Override
  protected JavaPairRDD<Tuple3<String, Long, String>, SpecificRecord>
    expandGvcfBlocks(JavaPairRDD<Tuple3<String, Long, String>, SpecificRecord> records,
      long segmentSize) {
    return records.flatMapToPair(new ExpandGvcfBlocksFn(segmentSize));
  }

  private static class ExpandGvcfBlocksFn implements
      PairFlatMapFunction<Tuple2<Tuple3<String, Long, String>, SpecificRecord>,
          Tuple3<String, Long, String>, SpecificRecord> {

    private long segmentSize;

    public ExpandGvcfBlocksFn(long segmentSize) {
      this.segmentSize = segmentSize;
    }

    @Override
    public Iterable<Tuple2<Tuple3<String, Long, String>, SpecificRecord>>
      call(Tuple2<Tuple3<String, Long, String>, SpecificRecord> input) {

      SpecificRecord record = input._2();
      if (record instanceof FlatVariantCall) {
        FlatVariantCall v = (FlatVariantCall) record;
        boolean block = v.getAlternateBases1().equals("") // <NON_REF>
            && v.getAlternateBases2() == null;
        if (block) {
          List<Tuple2<Tuple3<String, Long, String>, SpecificRecord>> tuples =
              Lists.newArrayList();
          Tuple3<String, Long, String> key = input._1();
          long start = v.getStart();
          long end = v.getEnd();
          long segmentStart = getRangeStart(segmentSize, start);
          long segmentEnd = getRangeStart(segmentSize, end);
          for (long pos = start; pos < end; pos++) {
            FlatVariantCall vCopy = SpecificData.get().deepCopy(v.getSchema(), v);
            vCopy.setStart(pos);
            vCopy.setEnd(pos + 1);
            if (pos > start) {
              // set reference to unknown (TODO: broadcast ref so we can set correctly)
              vCopy.setReferenceBases(""); // set reference to unknown
            }
            Tuple3<String, Long, String> newKey = (segmentStart == segmentEnd) ?
                  // block is contained in one segment
                  key :
                  // block spans multiple segments, so need to update key with correct pos
                  new Tuple3<>(key._1(), getRangeStart(segmentSize, pos), key._3());
            tuples.add(new Tuple2<>(newKey, (SpecificRecord) vCopy));
          }
          return tuples;
        }
      }
      // TODO: Variant
      return ImmutableList.of(input);
    }
  }
}
