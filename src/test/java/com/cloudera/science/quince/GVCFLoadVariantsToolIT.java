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

import java.io.File;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;

import static com.cloudera.science.quince.LoadVariantsToolIT.checkSortedByStart;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GVCFLoadVariantsToolIT {

  private LoadVariantsTool tool;

  @Rule
  public final SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  @Before
  public void setUp() {
    tool = new LoadVariantsTool();
    Configuration conf = new Configuration();
    tool.setConf(conf);
  }

  @Test
  public void testExpandGvcfBlocks() throws Exception {

    String baseDir = "target/datasets";

    FileUtil.fullyDelete(new File(baseDir));

    String sampleGroup = "sample1";
    String input = "datasets/variants_gvcf";
    String output = "target/datasets/variants_flat_locuspart_gvcf";

    int exitCode = tool.run(new String[]{"--flatten", "--sample-group", sampleGroup,
        "--expand-gvcf-blocks", input, output});

    assertEquals(0, exitCode);
    File partition1 = new File(baseDir,
        "variants_flat_locuspart_gvcf/chr=20/pos=9000000/sample_group=sample1");
    assertTrue(partition1.exists());

    File[] dataFiles1 = partition1.listFiles(new LoadVariantsToolIT.HiddenFileFilter());

    assertEquals(1, dataFiles1.length);
    assertTrue(dataFiles1[0].getName().endsWith(".parquet"));

    ParquetReader<GenericRecord> parquetReader1 =
        AvroParquetReader.<GenericRecord>builder(new Path(dataFiles1[0].toURI())).build();

    // first record is a non-variant site record
    GenericRecord flat1 = parquetReader1.read();
    assertEquals(".", flat1.get("id"));
    assertEquals("20", flat1.get("referenceName"));
    assertEquals(9999999L, flat1.get("start"));
    assertEquals(10000000L, flat1.get("end"));
    assertEquals("T", flat1.get("referenceBases"));
    assertEquals("", flat1.get("alternateBases_1"));
    assertEquals("NA12878", flat1.get("callSetId"));
    assertEquals(0, flat1.get("genotype_1"));
    assertEquals(0, flat1.get("genotype_2"));

    checkSortedByStart(dataFiles1[0], 1);

    File partition2 = new File(baseDir,
        "variants_flat_locuspart_gvcf/chr=20/pos=10000000/sample_group=sample1");
    assertTrue(partition1.exists());

    File[] dataFiles2 = partition2.listFiles(new LoadVariantsToolIT.HiddenFileFilter());

    assertEquals(1, dataFiles2.length);
    assertTrue(dataFiles1[0].getName().endsWith(".parquet"));

    ParquetReader<GenericRecord> parquetReader2 =
        AvroParquetReader.<GenericRecord>builder(new Path(dataFiles2[0].toURI())).build();

    // second record is a non-variant site record, but in a new segment
    GenericRecord flat2 = parquetReader2.read();
    assertEquals(".", flat2.get("id"));
    assertEquals("20", flat2.get("referenceName"));
    assertEquals(10000000L, flat2.get("start"));
    assertEquals(10000001L, flat2.get("end"));
    assertEquals("", flat2.get("referenceBases"));
    assertEquals("", flat2.get("alternateBases_1"));
    assertEquals("NA12878", flat2.get("callSetId"));
    assertEquals(0, flat2.get("genotype_1"));
    assertEquals(0, flat2.get("genotype_2"));

    for (int i = 0; i < 114; i++) {
      parquetReader2.read();
    }

    // last record in the first block
    GenericRecord flat3 = parquetReader2.read();
    assertEquals(".", flat3.get("id"));
    assertEquals("20", flat3.get("referenceName"));
    assertEquals(10000115L, flat3.get("start"));
    assertEquals(10000116L, flat3.get("end"));
    assertEquals("", flat3.get("referenceBases"));
    assertEquals("", flat3.get("alternateBases_1"));
    assertEquals("NA12878", flat3.get("callSetId"));
    assertEquals(0, flat3.get("genotype_1"));
    assertEquals(0, flat3.get("genotype_2"));

    // regular variant call
    GenericRecord flat4 = parquetReader2.read();
    assertEquals(".", flat4.get("id"));
    assertEquals("20", flat4.get("referenceName"));
    assertEquals(10000116L, flat4.get("start"));
    assertEquals(10000117L, flat4.get("end"));
    assertEquals("C", flat4.get("referenceBases"));
    assertEquals("T", flat4.get("alternateBases_1"));
    assertEquals("NA12878", flat4.get("callSetId"));
    assertEquals(0, flat4.get("genotype_1"));
    assertEquals(1, flat4.get("genotype_2"));

    checkSortedByStart(dataFiles2[0], 1437);

  }

}
