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

package com.cloudera.science.quince.crunch;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.Target;
import org.apache.crunch.io.CrunchOutputs;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.MapReduceTarget;
import org.apache.crunch.io.OutputHandler;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.kududb.client.Operation;
import org.kududb.mapreduce.KuduTableMapReduceUtil;
import org.kududb.mapreduce.KuduTableOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Based on HBaseTarget from Crunch
public class KuduTarget implements MapReduceTarget {

  private static final Logger LOG = LoggerFactory.getLogger(KuduTarget.class);

  private String kuduMasterAddresses;
  protected String table;
  private Map<String, String> extraConf = Maps.newHashMap();

  public KuduTarget(String kuduMasterAddresses, String table) {
    this.kuduMasterAddresses = kuduMasterAddresses;
    this.table = table;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null) {
      return false;
    }
    if (!other.getClass().equals(getClass())) {
      return false;
    }
    KuduTarget o = (KuduTarget) other;
    return table.equals(o.table);
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    return hcb.append(table).toHashCode();
  }

  @Override
  public String toString() {
    return "KuduTable(" + table + ")";
  }

  @Override
  public boolean accept(OutputHandler handler, PType<?> ptype) {
    if (Operation.class.equals(ptype.getTypeClass())) {
      handler.configure(this, ptype);
      return true;
    }
    return false;
  }

  @Override
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {
    final Configuration conf = job.getConfiguration();
    conf.setStrings("io.serializations", conf.get("io.serializations"),
        OperationSerialization.class.getName());

    FileOutputFormat.setOutputPath(job, outputPath);

    if (null == name) {

      job.setOutputFormatClass(KuduTableOutputFormat.class);
      job.setJarByClass(KuduTableOutputFormat.class);
      try {
        new KuduTableMapReduceUtil.TableOutputFormatConfigurator(job, table, kuduMasterAddresses)
            .addDependencies(true)
            .configure();
      } catch (IOException e) {
        throw new CrunchRuntimeException(e);
      }

      for (Map.Entry<String, String> e : extraConf.entrySet()) {
        conf.set(e.getKey(), e.getValue());
      }
    } else {
      FormatBundle<KuduTableOutputFormat> bundle =
          FormatBundle.forOutput(KuduTableOutputFormat.class);

      // can't use KuduTableMapReduceUtil here since we are using a FormatBundle
      bundle.set("kudu.mapreduce.master.addresses", kuduMasterAddresses);
      bundle.set("kudu.mapreduce.output.table", table);

      // set on the conf too so that we can access from KuduFn to create a table client
      conf.set("kudu.mapreduce.master.addresses", kuduMasterAddresses);
      conf.set("kudu.mapreduce.output.table", table);

      for (Map.Entry<String, String> e : extraConf.entrySet()) {
        bundle.set(e.getKey(), e.getValue());
      }
      CrunchOutputs.addNamedOutput(job, name,
          bundle,
          NullWritable.class,
          Operation.class);
    }
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(PType<T> ptype) {
    return null;
  }

  @Override
  public Target outputConf(String key, String value) {
    extraConf.put(key, value);
    return this;
  }

  @Override
  public boolean handleExisting(WriteMode strategy, long lastModifiedAt, Configuration conf) {
    LOG.info("KuduTarget ignores checks for existing outputs...");
    return false;
  }

  @Override
  public Converter<?, ?, ?, ?> getConverter(final PType<?> ptype) {
    if (Operation.class.equals(ptype.getTypeClass())) {
      return new KuduValueConverter<Operation>(Operation.class);
    } else {
      throw new IllegalArgumentException("KuduTarget only supports Operation, not: " +
          ptype.getTypeClass());
    }
  }
}
