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

import java.util.ArrayList;
import org.kududb.ColumnSchema;
import org.kududb.ColumnSchema.ColumnSchemaBuilder;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.client.KuduClient;

public final class KuduUtils {

  private KuduUtils() {
  }

  public static void createTableIfNecessary(String masterAddress, String tableName)
      throws Exception {

    KuduClient client = new KuduClient.KuduClientBuilder(masterAddress).build();

    if (client.tableExists(tableName)) {
      return;
    }

    // columns match those in FlatVariantCall
    // key is (referenceName, start, sampleGroup)
    ArrayList<ColumnSchema> cols = new ArrayList<>();
    cols.add(new ColumnSchemaBuilder("referenceName", Type.STRING).key(true).build());
    cols.add(new ColumnSchemaBuilder("start", Type.INT64).key(true).build());
    cols.add(new ColumnSchemaBuilder("sampleGroup", Type.STRING).key(true).build());
    cols.add(new ColumnSchemaBuilder("callSetId", Type.STRING).key(true).build());

    cols.add(new ColumnSchemaBuilder("id", Type.STRING).build());
    cols.add(new ColumnSchemaBuilder("variantSetId", Type.STRING).build());
    cols.add(new ColumnSchemaBuilder("names_1", Type.STRING).nullable(true).build());
    cols.add(new ColumnSchemaBuilder("names_2", Type.STRING).nullable(true).build());
    cols.add(new ColumnSchemaBuilder("created", Type.INT64).nullable(true).build());
    cols.add(new ColumnSchemaBuilder("updated", Type.INT64).nullable(true).build());
    cols.add(new ColumnSchemaBuilder("end", Type.INT64).build());
    cols.add(new ColumnSchemaBuilder("referenceBases", Type.STRING).build());
    cols.add(new ColumnSchemaBuilder("alternateBases_1", Type.STRING).build());
    cols.add(new ColumnSchemaBuilder("alternateBases_2", Type.STRING).nullable(true).build());
    cols.add(new ColumnSchemaBuilder("alleleIds_1", Type.STRING).nullable(true).build());
    cols.add(new ColumnSchemaBuilder("alleleIds_2", Type.STRING).nullable(true).build());
    cols.add(new ColumnSchemaBuilder("callSetName", Type.STRING).nullable(true).build());
    cols.add(new ColumnSchemaBuilder("variantId", Type.STRING).nullable(true).build());
    cols.add(new ColumnSchemaBuilder("genotype_1", Type.INT32).nullable(true).build());
    cols.add(new ColumnSchemaBuilder("genotype_2", Type.INT32).nullable(true).build());
    cols.add(new ColumnSchemaBuilder("genotypeLikelihood_1", Type.DOUBLE).nullable(true).build());
    cols.add(new ColumnSchemaBuilder("genotypeLikelihood_2", Type.DOUBLE).nullable(true).build());

    client.createTable(tableName, new Schema(cols));
  }

}
