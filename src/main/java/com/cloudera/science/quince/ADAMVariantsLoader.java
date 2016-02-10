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
import java.util.Collection;
import java.util.Set;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bdgenomics.formats.avro.FlatGenotype;
import org.bdgenomics.formats.avro.FlatVariant;
import org.bdgenomics.formats.avro.Genotype;
import org.bdgenomics.formats.avro.Variant;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import scala.Tuple3;

public class ADAMVariantsLoader extends VariantsLoader {
  @Override
  protected JavaPairRDD<Tuple3<String, Long, String>, SpecificRecord>
    loadKeyedRecords(String inputFormat, Path inputPath, Configuration conf,
        JavaSparkContext context, boolean variantsOnly, boolean flatten, String sampleGroup,
        Set<String> samples)
        throws IOException {
    JavaPairRDD<org.bdgenomics.formats.avro.Variant, Collection<Genotype>> adamRecords =
        readVariants(inputFormat, inputPath, conf, context, sampleGroup);

    // The data are now loaded into ADAM variant objects; convert to keyed SpecificRecords
    ADAMToKeyedSpecificRecordFn converter =
        new ADAMToKeyedSpecificRecordFn(variantsOnly, flatten, sampleGroup, samples);

    return adamRecords.flatMapToPair(converter);
  }

  @Override
  protected Class getSpecificRecordType(boolean variantsOnly, boolean flatten) {
    if (variantsOnly && flatten) {
      return FlatVariant.class;
    } else if (variantsOnly && !flatten) {
      return Variant.class;
    } else if (!variantsOnly && flatten) {
      return FlatGenotype.class;
    } else {  // !variantsOnly && !flatten
      return Genotype.class;
    }
  }

  /*
   * Read input files (which may be VCF, Avro, or Parquet) and return a PCollection
   * of ADAM Variant/Genotype pairs.
   */
  private static JavaPairRDD<Variant, Collection<Genotype>>
      readVariants(String inputFormat, Path inputPath, Configuration conf,
      JavaSparkContext context, String sampleGroup) throws IOException {
    JavaPairRDD<Variant, Collection<Genotype>> adamRecords;
    if (inputFormat.equals("VCF")) {
      JavaPairRDD<LongWritable, VariantContextWritable>
          vcfRecords = context.newAPIHadoopFile(inputPath.toString(),
            VCFInputFormat.class, LongWritable.class, VariantContextWritable.class, conf);
      adamRecords = vcfRecords.values().flatMapToPair(new VCFToADAMVariantFn());
    } else if (inputFormat.equals("AVRO")) {
      throw new UnsupportedOperationException("Unsupported input format: " + inputFormat);
    } else if (inputFormat.equals("PARQUET")) {
      throw new UnsupportedOperationException("Unsupported input format: " + inputFormat);
    } else {
      throw new IllegalStateException("Unrecognized input format: " + inputFormat);
    }
    return adamRecords;
  }
}
