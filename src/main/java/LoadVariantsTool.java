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

import java.io.IOException;
import java.io.InputStream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Source;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.parquet.AvroParquetFileSource;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.ga4gh.models.FlatVariant;
import org.ga4gh.models.Variant;
import org.kitesdk.data.CompressionType;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.View;
import org.kitesdk.data.crunch.CrunchDatasets;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;
import org.kitesdk.data.spi.PartitionStrategyParser;

/**
 * Loads Variants stored in Avro or Parquet GA4GH format into a Hadoop filesystem,
 * ready for querying with Hive or Impala.
 */
public class LoadVariantsTool extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println("Usage: " + getClass().getSimpleName() +
          " <partition-strategy> <sample-group> <input-path> <output-path>");
      System.exit(1);
    }
    String partitionStrategyName = args[0];
    String sampleGroup = args[1];
    String inputPath = args[2];
    String outputPath = args[3];

    Configuration conf = getConf();
    // Copy records to avoid problem with Parquet string statistics not being correct.
    // This can be removed from parquet 1.8.0
    // (see https://issues.apache.org/jira/browse/PARQUET-251).
    conf.setBoolean(DatasetKeyOutputFormat.KITE_COPY_RECORDS, true);

    Path path = new Path(inputPath);
    Source<Variant> source = readSource(path, conf);

    Pipeline pipeline = new MRPipeline(getClass(), conf);
    PCollection<Variant> records = pipeline.read(source);

    PCollection<FlatVariant> flatRecords = records.parallelDo(
        new FlattenVariantFn(), Avros.specifics(FlatVariant.class));

    DatasetDescriptor desc = new DatasetDescriptor.Builder()
        .schema(FlatVariant.getClassSchema())
        .partitionStrategy(readPartitionStrategy(partitionStrategyName))
        .format(Formats.PARQUET)
        .compressionType(CompressionType.Uncompressed)
        .build();

    View<FlatVariant> dataset = Datasets.create(outputPath, desc,
        FlatVariant.class).getDataset().with("sample_group", sampleGroup);

    int numReducers = conf.getInt("mapreduce.job.reduces", 1);
    System.out.println("Num reducers: " + numReducers);

    final Schema sortKeySchema = SchemaBuilder.record("sortKey")
        .fields().requiredString("sampleId").endRecord();

    PCollection<FlatVariant> partitioned =
        CrunchDatasets.partitionAndSort(flatRecords, dataset, new
            FlatVariantRecordMapFn(sortKeySchema), sortKeySchema, numReducers, 1);

    pipeline.write(partitioned, CrunchDatasets.asTarget(dataset));

    PipelineResult result = pipeline.done();
    return result.succeeded() ? 0 : 1;

  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new LoadVariantsTool(), args);
    System.exit(exitCode);
  }

  private static Source<Variant> readSource(Path path, Configuration conf) throws
      IOException {
    Path file = SchemaUtils.findFile(path, conf);
    Format format = SchemaUtils.readFormat(file);
    if (format == Formats.AVRO) {
      return From.avroFile(path, Avros.specifics(Variant.class));
    } else if (format == Formats.PARQUET) {
      return new AvroParquetFileSource(path, Avros.specifics(Variant.class));
    }
    throw new IllegalStateException("Unrecognized format for " + file);
  }

  static PartitionStrategy readPartitionStrategy(String name) throws IOException {
    InputStream in = LoadVariantsTool.class.getResourceAsStream(name + ".json");
    try {
      return PartitionStrategyParser.parse(in);
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }

  private static class FlatVariantRecordMapFn extends MapFn<FlatVariant, GenericData.Record> {

    private final String sortKeySchemaString; // TODO: improve

    public FlatVariantRecordMapFn(Schema sortKeySchema) {
      this.sortKeySchemaString = sortKeySchema.toString();
    }

    @Override
    public GenericData.Record map(FlatVariant input) {
      GenericData.Record record = new GenericData.Record(new Schema.Parser().parse(sortKeySchemaString));
      record.put("sampleId", input.getCallSetId());
      return record;
    }
  }
}