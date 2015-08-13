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
import com.cloudera.science.quince.crunch.KuduTarget;
import com.cloudera.science.quince.crunch.KuduTypes;
import java.util.List;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.ga4gh.models.FlatVariantCall;
import org.ga4gh.models.Variant;
import org.kududb.client.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads Variants stored in VCF, or Avro or Parquet GA4GH format, into Kudu, ready for
 * querying with Impala.
 */
@Parameters(commandDescription = "Load variants tool (for Kudu)")
public class KuduLoadVariantsTool extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(KuduLoadVariantsTool.class);

  @Parameter(description="<input-path>")
  private List<String> paths;

  @Parameter(names="--overwrite",
      description="Allow data for an existing sample group to be overwritten.")
  private boolean overwrite = false;

  @Parameter(names="--sample-group",
      description="An identifier for the group of samples being loaded.")
  private String sampleGroup = "default";

  @Parameter(names="--kudu-table",
      description="The name of the Kudu table to store variants in.")
  private String tableName = "variants";

  @Parameter(names="--kudu-master",
      description="The Kudu master address.")
  private String kuduMasterAddress = "quickstart.cloudera";

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

    Configuration conf = getConf();
    Path inputPath = new Path(inputPathString);

    Path file = FileUtils.findFile(inputPath, conf);
    if (file.getName().endsWith(".vcf")) {
      VariantContextToVariantFn.configureHeaders(conf,
          FileUtils.findVcfs(inputPath, conf), sampleGroup);
    }

    KuduUtils.createTableIfNecessary(kuduMasterAddress, tableName);

    Pipeline pipeline = new MRPipeline(getClass(), conf);

    PCollection<Variant> records = FileUtils.readVariants(inputPath, conf, pipeline);

    PCollection<FlatVariantCall> flatRecords = records.parallelDo(
        new FlattenVariantFn(), Avros.specifics(FlatVariantCall.class));

    PCollection<Operation> table = flatRecords.parallelDo(new KuduFn(sampleGroup),
        KuduTypes.operations());

    try {
      Target.WriteMode writeMode =
          overwrite ? Target.WriteMode.OVERWRITE : Target.WriteMode.DEFAULT;
      pipeline.write(table, new KuduTarget(kuduMasterAddress, tableName), writeMode);
    } catch (CrunchRuntimeException e) {
      LOG.error("Crunch runtime error", e);
      return 1;
    }

    PipelineResult result = pipeline.done();
    return result.succeeded() ? 0 : 1;

  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new KuduLoadVariantsTool(), args);
    System.exit(exitCode);
  }
}
