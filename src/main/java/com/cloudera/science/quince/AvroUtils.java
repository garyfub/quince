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

import com.databricks.spark.avro.SchemaConverters;
import com.databricks.spark.avro.SchemaConverters$;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

public final class AvroUtils {

  public static final String CHR_COLUMN = "chr";
  public static final String POS_COLUMN = "pos";
  public static final String SAMPLE_GROUP_COLUMN = "sample_group";

  private AvroUtils() {
  }

  public static <T extends IndexedRecord> Schema getAvroSchema(Class clazz) {
    @SuppressWarnings("unchecked")
    T t = (T) ReflectionUtils.newInstance(clazz, null);
    return t.getSchema();
  }

  public static Schema.Field cloneField(Schema.Field field) {
    return new Schema.Field(field.name(), field.schema(), field.doc(),
        field.defaultValue());
  }

  public static Schema addPartitionColumns(Schema schema, boolean includeSampleGroup) {
    Schema newSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema
        .getNamespace(), schema.isError());
    List<Schema.Field> fields = schema.getFields();
    List<Schema.Field> newFields = new ArrayList<>();
    for (Schema.Field field : fields) {
      newFields.add(cloneField(field));
    }
    newFields.add(new Schema.Field(CHR_COLUMN, Schema.create(Schema.Type.STRING), null, null));
    newFields.add(new Schema.Field(POS_COLUMN, Schema.create(Schema.Type.LONG), null, null));
    if (includeSampleGroup) {
      newFields.add(new Schema.Field(SAMPLE_GROUP_COLUMN,
          Schema.create(Schema.Type.STRING), null, null));
    }
    newSchema.setFields(newFields);
    return newSchema;
  }

  public static GenericRecord addPartitionColumns(SpecificRecord record, Schema
      newSchema, String chr, long pos, String sampleGroup) {
    @SuppressWarnings("unchecked")
    GenericRecord newRecord = (GenericRecord) GenericData.get().newRecord(null, newSchema);
    for (Schema.Field f : record.getSchema().getFields()) {
      newRecord.put(f.pos(), record.get(f.pos())); // TODO: don't assume specific and
      // generic types are the same (can't use deep copy since the schemas are different)
    }
    newRecord.put(CHR_COLUMN, chr);
    newRecord.put(POS_COLUMN, pos);
    if (sampleGroup != null) {
      newRecord.put(SAMPLE_GROUP_COLUMN, sampleGroup);
    }
    return newRecord;
  }

  public static <T extends IndexedRecord> DataFrame createDataFrame(
      JavaSparkContext context, JavaRDD<T> rdd, Schema schema) {
    SQLContext sqlContext = new SQLContext(context);
    // Need to go through MODULE$ since SchemaConverters#toSqlType is not public.
    SchemaConverters.SchemaType schemaType =
        SchemaConverters$.MODULE$.toSqlType(schema);
    StructType structType = (StructType) schemaType.dataType();
    // Need to go through MODULE$ since SchemaConverters#createConverterToSQL is not
    // public. Note that https://github.com/databricks/spark-avro/pull/89 proposes to
    // make it public, but it's not going to be merged.
    final Function1<Object, Object> converter =
        SchemaConverters$.MODULE$.createConverterToSQL(schema);
    JavaRDD<Row> rows = rdd.map(new Function<T, Row>() {
      @Override
      @SuppressWarnings("unchecked")
      public Row call(T record) throws Exception {
        return (Row) converter.apply(record);
      }
    });
    return sqlContext.createDataFrame(rows, structType);
  }
}
