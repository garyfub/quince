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

import org.apache.crunch.MapFn;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.ga4gh.models.FlatVariantCall;
import org.kududb.client.Insert;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduTable;
import org.kududb.client.Operation;
import org.kududb.client.PartialRow;

class KuduFn extends MapFn<FlatVariantCall, Operation> {

  private transient KuduTable table;
  private String sampleGroup;

  public KuduFn(String sampleGroup) {
    this.sampleGroup = sampleGroup;
  }

  @Override
  public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
    super.setContext(context);
    // The following doesn't work since Crunch constructs OutputFormats lazily, only
    // when writing records (see CrunchOutputs#write). This means that
    // KuduTableOutputFormat#setConf which sets up the table has not had a chance to
    // run.

    //table = KuduTableMapReduceUtil.getTableFromContext(context);

    // instead, just open the table directly
    String masterAddress = context.getConfiguration().get("kudu.mapreduce.master.addresses");
    String tableName = context.getConfiguration().get("kudu.mapreduce.output.table");
    KuduClient client = new KuduClient.KuduClientBuilder(masterAddress)
        .build();
    try {
      table = client.openTable(tableName);
    } catch (Exception ex) {
      throw new RuntimeException("Could not obtain the table from the master, " +
          "is the master running and is this table created? tablename=" + tableName + " and " +
          "master address= " + masterAddress, ex);
    }
  }

  public Operation map(FlatVariantCall variant) {
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();

    // key fields
    row.addString("referenceName", variant.getReferenceName().toString());
    row.addLong("start", variant.getStart());
    row.addString("sampleGroup", sampleGroup);
    row.addString("callSetId", variant.getCallSetId().toString());

    // other fields
    row.addString("id", variant.getId().toString());
    row.addString("variantSetId", variant.getVariantSetId().toString());
    addStringIfNotNull(row, "names_1", variant.getNames1());
    addStringIfNotNull(row, "names_2", variant.getNames2());
    addLongIfNotNull(row, "created", variant.getCreated());
    addLongIfNotNull(row, "updated", variant.getUpdated());
    row.addLong("end", variant.getEnd());
    row.addString("referenceBases", variant.getReferenceBases().toString());
    row.addString("alternateBases_1", variant.getAlternateBases1().toString());
    addStringIfNotNull(row, "alternateBases_2", variant.getAlternateBases2());
    addStringIfNotNull(row, "alleleIds_1", variant.getAlleleIds1());
    addStringIfNotNull(row, "alleleIds_2", variant.getAlleleIds2());
    addStringIfNotNull(row, "callSetName", variant.getCallSetName());

    addStringIfNotNull(row, "variantId", variant.getVariantId());
    addIntIfNotNull(row, "genotype_1", variant.getGenotype1());
    addIntIfNotNull(row, "genotype_2", variant.getGenotype2());
    addDoubleIfNotNull(row, "genotypeLikelihood_1", variant.getGenotypeLikelihood1());
    addDoubleIfNotNull(row, "genotypeLikelihood_2", variant.getGenotypeLikelihood2());

    return insert;
  }

  private void addIntIfNotNull(PartialRow row, String columnName, Integer value) {
    if (value != null) {
      row.addInt(columnName, value);
    }
  }

  private void addLongIfNotNull(PartialRow row, String columnName, Long value) {
    if (value != null) {
      row.addLong(columnName, value);
    }
  }

  private void addDoubleIfNotNull(PartialRow row, String columnName, Double value) {
    if (value != null) {
      row.addDouble(columnName, value);
    }
  }

  private void addStringIfNotNull(PartialRow row, String columnName, CharSequence value) {
    if (value != null) {
      row.addString(columnName, value.toString());
    }
  }
}
