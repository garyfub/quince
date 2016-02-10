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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads Variants stored in VCF, or Avro or Parquet GA4GH format, into a Hadoop
 * filesystem, ready for querying with Hive or Impala.
 */
@Parameters(commandDescription = "Load variants tool")
public class LoadVariantsTool extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(LoadVariantsTool.class);

  @Parameter(description="<input-path> <output-path>")
  private List<String> paths;

  @Parameter(names="--data-model", description="The variant data model (GA4GH, or ADAM)")
  private String dataModel = "GA4GH";

  @Parameter(names="--input-format", description="Format of input data (VCF, AVRO, or PARQUET)")
  private String inputFormat = "VCF";

  @Parameter(names="--overwrite",
      description="Allow data for an existing sample group to be overwritten.")
  private boolean overwrite = false;

  @Parameter(names="--sample-group",
      description="An identifier for the group of samples being loaded.")
  private String sampleGroup = null;

  @Parameter(names="--variants-only",
      description="Ignore samples and only load variants.")
  private boolean variantsOnly = false;

  @Parameter(names="--samples",
      description="Comma-separated list of samples to include.")
  private String samples;

  @Parameter(names="--segment-size",
      description="The number of base pairs in each segment partition.")
  private long segmentSize = 1000000;

  @Parameter(names="--redistribute",
      description="Whether to repartition the data by locus/sample group.")
  private boolean redistribute = false;

  @Parameter(names="--flatten",
      description="Whether to flatten the data types.")
  private boolean flatten = false;

  @Parameter(names="--num-reducers",
      description="The number of reducers to use.")
  private int numReducers = -1;

  @Parameter(names="--spark-master",
      description="The Spark Master.")
  private String sparkMaster = "local";

  @Override
  public int run(String[] args) throws Exception {
    JCommander jc = new JCommander(this);
    try {
      jc.parse(args);
    } catch (ParameterException e) {
      jc.usage();
      return 1;
    }

    if (paths == null || paths.size() != 2) {
      jc.usage();
      return 1;
    }

    String inputPathString = paths.get(0);
    String outputPathString = paths.get(1);

    Configuration conf = getConf();
    Path inputPath = new Path(inputPathString);
    Path outputPath = new Path(outputPathString);
    outputPath = outputPath.getFileSystem(conf).makeQualified(outputPath);

    SparkConf sparkConf = new SparkConf()
        .setMaster(sparkMaster)
        .setAppName(getClass().getSimpleName())
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.sql.parquet.compression.codec", "uncompressed");
    // TODO: add command line args

    JavaSparkContext context = null;
    try {
      context = new JavaSparkContext(sparkConf);

      VariantsLoader variantsLoader;
      if (dataModel.equals("GA4GH")) {
        variantsLoader = new GA4GHVariantsLoader();
      } else if (dataModel.equals("ADAM")) {
        variantsLoader = new ADAMVariantsLoader();
      } else {
        jc.usage();
        return 1;
      }

      Set<String> sampleSet = samples == null ? null :
          Sets.newLinkedHashSet(Splitter.on(',').split(samples));

      JavaRDD<GenericRecord> partitionKeyedRecords =
          variantsLoader.loadPartitionedVariants(inputFormat, inputPath, conf, context,
              variantsOnly, flatten, sampleGroup, sampleSet, redistribute, segmentSize,
              numReducers);

      if (FileUtils.sampleGroupExists(outputPath, conf, sampleGroup)) {
        if (overwrite) {
          FileUtils.deleteSampleGroup(outputPath, conf, sampleGroup);
        } else {
          LOG.error("Sample group already exists: " + sampleGroup);
          return 1;
        }
      }

      Class specificRecordType = variantsLoader.getSpecificRecordType(variantsOnly,
          flatten);
      Schema specificRecordSchema = AvroUtils.getAvroSchema(specificRecordType);
      Schema schema = AvroUtils.addPartitionColumns(specificRecordSchema,
          sampleGroup != null);
      DataFrame df = AvroUtils.createDataFrame(context, partitionKeyedRecords, schema);
      String[] partitionColumns = sampleGroup == null ?
          new String[] {AvroUtils.CHR_COLUMN, AvroUtils.POS_COLUMN} :
          new String[] {AvroUtils.CHR_COLUMN, AvroUtils.POS_COLUMN, AvroUtils.SAMPLE_GROUP_COLUMN};
      df.write()
          .mode(SaveMode.Append)
          .partitionBy(partitionColumns)
          .parquet(outputPath.toString());

    } finally {
      context.close();
    }

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new LoadVariantsTool(), args);
    System.exit(exitCode);
  }

}
