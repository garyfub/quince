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
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LoadVariantsToolIT {

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
  public void testMissingPaths() throws Exception {
    int exitCode = tool.run(new String[0]);
    assertEquals(1, exitCode);
    assertTrue(systemOutRule.getLog().startsWith(
        "Usage: <main class> [options] <input-path> <output-path>"));
  }

  @Test
  public void testInvalidOption() throws Exception {
    int exitCode = tool.run(new String[]{"--invalid", "blah", "foo", "bar"});
    assertEquals(1, exitCode);
    assertTrue(systemOutRule.getLog().startsWith(
        "Usage: <main class> [options] <input-path> <output-path>"));
  }

  @Test
  public void testOverwrite() throws Exception {

    String baseDir = "target/datasets";

    FileUtil.fullyDelete(new File(baseDir));

    String sampleGroup = "default";
    String input = "datasets/variants_vcf/small.vcf";
    String output = "target/datasets/variants_out";

    int exitCode = tool.run(
        new String[]{ "--flatten", "--sample-group", sampleGroup, input, output });

    assertEquals(0, exitCode);
    File partition1 = new File(baseDir,
        "variants_out/chr=1/pos=0/sample_group=default");
    assertTrue(partition1.exists());

    File[] dataFiles = partition1.listFiles(new HiddenFileFilter());

    assertEquals(1, dataFiles.length);
    assertTrue(dataFiles[0].getName().endsWith(".parquet"));

    // loading into the same sample group again should fail
    exitCode = tool.run(new String[]{ "--flatten", "--sample-group", sampleGroup, input, output });
    assertEquals(1, exitCode);

    // unless the overwrite option is specified
    exitCode = tool.run(
        new String[]{ "--overwrite", "--flatten", "--sample-group", sampleGroup, input, output });
    assertEquals(0, exitCode);
    assertTrue(partition1.exists());

    // loading into a new sample group should always succeed
    exitCode = tool.run(new String[]{ "--flatten", "--sample-group", "sample2", input, output });
    assertEquals(0, exitCode);
    File partition2 = new File(baseDir,
        "variants_out/chr=1/pos=0/sample_group=sample2");
    assertTrue(partition1.exists());
    assertTrue(partition2.exists());

    // overwriting an existing sample group should leave others untouched
    exitCode = tool.run(
        new String[]{ "--overwrite", "--flatten", "--sample-group", sampleGroup, input, output });
    assertEquals(0, exitCode);
    assertTrue(partition1.exists());
    assertTrue(partition2.exists());
  }

  @Test
  public void testAvroInput() throws Exception {
    String baseDir = "target/datasets";

    FileUtil.fullyDelete(new File(baseDir));

    String sampleGroup = "default";
    String input = "datasets/variants_avro";
    String output = "target/datasets/variants_out";

    int exitCode = tool.run(new String[]{"--flatten", "--data-model", "GA4GH",
        "--input-format", "AVRO", "--sample-group", sampleGroup, input, output});
    assertEquals(0, exitCode);
    File partition = new File(baseDir, "variants_out/chr=1/pos=0/sample_group=default");
    File[] dataFiles = partition.listFiles(new HiddenFileFilter());

    ParquetReader<GenericRecord> parquetReader =
        AvroParquetReader.<GenericRecord>builder(new Path(dataFiles[0].toURI())).build();

    // first record has first sample (call set) ID
    GenericRecord flat1 = parquetReader.read();
    assertEquals(".", flat1.get("id"));
    assertEquals("1", flat1.get("referenceName"));
    assertEquals(14396L, flat1.get("start"));
    assertEquals(14400L, flat1.get("end"));
    assertEquals("CTGT", flat1.get("referenceBases"));
    assertEquals("C", flat1.get("alternateBases_1"));
    assertEquals("NA12878", flat1.get("callSetId"));
    assertEquals(0, flat1.get("genotype_1"));
    assertEquals(1, flat1.get("genotype_2"));

    checkSortedByStart(dataFiles[0], 15);
  }

  @Test
  public void testParquetInput() throws Exception {
    // TODO
  }

  @Test
  public void testSmallVCF() throws Exception {
    String baseDir = "target/datasets";

    FileUtil.fullyDelete(new File(baseDir));

    String sampleGroup = "default";
    String input = "datasets/variants_vcf";
    String output = "target/datasets/variants_out";

    int exitCode = tool.run(new String[]{"--flatten", "--data-model", "GA4GH", "--sample-group",
        sampleGroup, input, output});
    assertEquals(0, exitCode);
    File partition = new File(baseDir, "variants_out/chr=1/pos=0/sample_group=default");
    File[] dataFiles = partition.listFiles(new HiddenFileFilter());

    ParquetReader<GenericRecord> parquetReader =
        AvroParquetReader.<GenericRecord>builder(new Path(dataFiles[0].toURI())).build();

    // first record has first sample (call set) ID
    GenericRecord flat1 = parquetReader.read();
    assertEquals(".", flat1.get("id"));
    assertEquals("1", flat1.get("referenceName"));
    assertEquals(14396L, flat1.get("start"));
    assertEquals(14400L, flat1.get("end"));
    assertEquals("CTGT", flat1.get("referenceBases"));
    assertEquals("C", flat1.get("alternateBases_1"));
    assertEquals("NA12878", flat1.get("callSetId"));
    assertEquals(0, flat1.get("genotype_1"));
    assertEquals(1, flat1.get("genotype_2"));

    checkSortedByStart(dataFiles[0], 15);
  }

  @Test
  public void testRestrictSamples() throws Exception {
    String baseDir = "target/datasets";

    FileUtil.fullyDelete(new File(baseDir));

    String sampleGroup = "default";
    String input = "datasets/variants_vcf";
    String output = "target/datasets/variants_out";

    int exitCode = tool.run(new String[]{"--flatten", "--data-model", "GA4GH", "--sample-group",
        sampleGroup, "--samples", "NA12878,NA12892", input, output});
    assertEquals(0, exitCode);
    File partition = new File(baseDir, "variants_out/chr=1/pos=0/sample_group=default");
    File[] dataFiles = partition.listFiles(new HiddenFileFilter());

    ParquetReader<GenericRecord> parquetReader =
        AvroParquetReader.<GenericRecord>builder(new Path(dataFiles[0].toURI())).build();

    // first record has first sample (call set) ID
    GenericRecord flat1 = parquetReader.read();
    assertEquals(".", flat1.get("id"));
    assertEquals("1", flat1.get("referenceName"));
    assertEquals(14396L, flat1.get("start"));
    assertEquals(14400L, flat1.get("end"));
    assertEquals("CTGT", flat1.get("referenceBases"));
    assertEquals("C", flat1.get("alternateBases_1"));
    assertEquals("NA12878", flat1.get("callSetId"));
    assertEquals(0, flat1.get("genotype_1"));
    assertEquals(1, flat1.get("genotype_2"));

    checkSortedByStart(dataFiles[0], 10);
  }

  @Test
  public void testDataModels() throws Exception {
    String baseDir = "target/datasets";

    FileUtil.fullyDelete(new File(baseDir));

    String sampleGroup = "default";
    String input = "datasets/variants_vcf";
    String output_flat_adam = "target/datasets/variants_flat_adam";
    String output_adam = "target/datasets/variants_adam";
    String output_flat_ga4gh = "target/datasets/variants_flat_ga4gh";
    String output_ga4gh = "target/datasets/variants_ga4gh";

    int exitCode;
    File partition;
    File[] dataFiles;

    exitCode = tool.run(new String[]{"--flatten", "--data-model", "ADAM", "--sample-group",
        sampleGroup, input, output_flat_adam});
    assertEquals(0, exitCode);
    partition = new File(output_flat_adam + "/chr=1/pos=0/sample_group=default");
    assertTrue(partition.exists());
    dataFiles = partition.listFiles(new HiddenFileFilter());
    assertEquals(1, dataFiles.length);
    assertTrue(dataFiles[0].getName().endsWith(".parquet"));

    exitCode = tool.run(new String[]{"--data-model", "ADAM", "--sample-group", sampleGroup, input,
        output_adam});
    assertEquals(0, exitCode);
    partition = new File(output_adam + "/chr=1/pos=0/sample_group=default");
    assertTrue(partition.exists());
    dataFiles = partition.listFiles(new HiddenFileFilter());
    assertEquals(1, dataFiles.length);
    assertTrue(dataFiles[0].getName().endsWith(".parquet"));

    exitCode = tool.run(new String[]{"--flatten", "--data-model", "GA4GH", "--sample-group",
        sampleGroup, input, output_flat_ga4gh});
    assertEquals(0, exitCode);
    partition = new File(output_flat_ga4gh + "/chr=1/pos=0/sample_group=default");
    assertTrue(partition.exists());
    dataFiles = partition.listFiles(new HiddenFileFilter());
    assertEquals(1, dataFiles.length);
    assertTrue(dataFiles[0].getName().endsWith(".parquet"));

    exitCode = tool.run(new String[]{"--data-model", "GA4GH", "--sample-group", sampleGroup, input,
        output_ga4gh});
    assertEquals(0, exitCode);
    partition = new File(output_ga4gh + "/chr=1/pos=0/sample_group=default");
    assertTrue(partition.exists());
    dataFiles = partition.listFiles(new HiddenFileFilter());
    assertEquals(1, dataFiles.length);
    assertTrue(dataFiles[0].getName().endsWith(".parquet"));
  }

  @Test
  public void testRedistribute() throws Exception {
    String baseDir = "target/datasets";

    FileUtil.fullyDelete(new File(baseDir));

    String sampleGroup = "default";
    String input = "datasets/variants_vcf";
    String output = "target/datasets/variants_out";

    int exitCode = tool.run(new String[]{"--redistribute", "--flatten", "--data-model", "GA4GH",
        "--sample-group", sampleGroup, input, output});
    assertEquals(0, exitCode);
    File partition = new File(baseDir, "variants_out/chr=1/pos=0/sample_group=default");
    assertTrue(partition.exists());
    File[] dataFiles = partition.listFiles(new HiddenFileFilter());
    assertEquals(1, dataFiles.length);
    assertTrue(dataFiles[0].getName().endsWith(".parquet"));

    ParquetReader<GenericRecord> parquetReader =
        AvroParquetReader.<GenericRecord>builder(new Path(dataFiles[0].toURI())).build();

    // variants should be sorted, so this is the first one that we should see
    GenericRecord fvc = parquetReader.read();
    assertEquals(".", fvc.get("id"));
    assertEquals("1", fvc.get("referenceName"));
    assertEquals(14396L, fvc.get("start"));
    assertEquals(14400L, fvc.get("end"));
    assertEquals("CTGT", fvc.get("referenceBases"));
    assertEquals("C", fvc.get("alternateBases_1"));
    assertEquals("NA12878", fvc.get("callSetId"));
    assertEquals(0, fvc.get("genotype_1"));
    assertEquals(1, fvc.get("genotype_2"));

    Set<String> observedSamples = new HashSet<>();
    int numCalls = 0;
    while (fvc != null) {
      observedSamples.add(fvc.get("callSetId").toString());
      numCalls += 1;
      fvc = parquetReader.read();
    }
    Set<String> expectedSamples = new HashSet<>(Arrays.asList("NA12878", "NA12891", "NA12892"));
    assertEquals(expectedSamples, observedSamples);
    assertEquals(15, numCalls);

    checkSortedByStart(dataFiles[0], 15);
  }

  @Test
  public void testNullSampleGroup() throws Exception {
    String baseDir = "target/datasets";

    FileUtil.fullyDelete(new File(baseDir));

    String input = "datasets/variants_vcf";
    String output = "target/datasets/variants_out";

    int exitCode = tool.run(new String[]{"--flatten", "--data-model", "GA4GH", input, output});
    assertEquals(0, exitCode);
    File partition = new File(baseDir, "variants_out/chr=1/pos=0");
    assertTrue(partition.exists());
    File[] dataFiles = partition.listFiles(new HiddenFileFilter());
    assertEquals(1, dataFiles.length);
    assertTrue(dataFiles[0].getName().endsWith(".parquet"));
  }

  @Test
  public void testVariantsOnly() throws Exception {

    String baseDir = "target/datasets";

    FileUtil.fullyDelete(new File(baseDir));

    String input = "datasets/variants_vcf";
    String output = "target/datasets/variants_flat_locuspart";

    int exitCode = tool.run(new String[]{"--variants-only", "--flatten", input, output});

    assertEquals(0, exitCode);
    File partition = new File(baseDir,
        "variants_flat_locuspart/chr=1/pos=0");
    assertTrue(partition.exists());

    File[] dataFiles = partition.listFiles(new HiddenFileFilter());

    assertEquals(1, dataFiles.length);
    assertTrue(dataFiles[0].getName().endsWith(".parquet"));

    ParquetReader<GenericRecord> parquetReader =
        AvroParquetReader.<GenericRecord>builder(new Path(dataFiles[0].toURI())).build();

    // first record has no sample (call set)
    GenericRecord flat1 = parquetReader.read();
    assertEquals(".", flat1.get("id"));
    assertEquals("1", flat1.get("referenceName"));
    assertEquals(14396L, flat1.get("start"));
    assertEquals(14400L, flat1.get("end"));
    assertEquals("CTGT", flat1.get("referenceBases"));
    assertEquals("C", flat1.get("alternateBases_1"));
    assertNull(flat1.get("callSetId"));
    assertNull(flat1.get("genotype_1"));
    assertNull(flat1.get("genotype_2"));

    checkSortedByStart(dataFiles[0], 5);
  }

  @Test
  public void testGVCF() throws Exception {

    // Note that sites with no variant calls are ignored, see https://github.com/cloudera/quince/issues/19

    String baseDir = "target/datasets";

    FileUtil.fullyDelete(new File(baseDir));

    String sampleGroup = "sample1";
    String input = "datasets/variants_gvcf";
    String output = "target/datasets/variants_flat_locuspart_gvcf";

    int exitCode = tool.run(new String[]{"--flatten", "--sample-group", sampleGroup, input, output});

    assertEquals(0, exitCode);
    File partition = new File(baseDir,
        "variants_flat_locuspart_gvcf/chr=20/pos=10000000/sample_group=sample1");
    assertTrue(partition.exists());

    File[] dataFiles = partition.listFiles(new HiddenFileFilter());

    assertEquals(1, dataFiles.length);
    assertTrue(dataFiles[0].getName().endsWith(".parquet"));

    ParquetReader<GenericRecord> parquetReader =
        AvroParquetReader.<GenericRecord>builder(new Path(dataFiles[0].toURI())).build();

    // first record has first sample (call set) ID
    GenericRecord flat1 = parquetReader.read();
    assertEquals(".", flat1.get("id"));
    assertEquals("20", flat1.get("referenceName"));
    assertEquals(10000116L, flat1.get("start"));
    assertEquals(10000117L, flat1.get("end"));
    assertEquals("C", flat1.get("referenceBases"));
    assertEquals("T", flat1.get("alternateBases_1"));
    assertEquals("NA12878", flat1.get("callSetId"));
    assertEquals(0, flat1.get("genotype_1"));
    assertEquals(1, flat1.get("genotype_2"));

    checkSortedByStart(dataFiles[0], 30);
  }

  static void checkSortedByStart(File file, int expectedCount) throws IOException {
    // check records are sorted by start position
    ParquetReader<GenericRecord> parquetReader =
        AvroParquetReader.<GenericRecord>builder(new Path(file.toURI())).build();

    int actualCount = 0;

    GenericRecord flat1 = parquetReader.read();
    actualCount++;

    @SuppressWarnings("unchecked")
    Long previousStart = (Long) flat1.get("start");
    while (true) {
      GenericRecord flat = parquetReader.read();
      if (flat == null) {
        break;
      }
      @SuppressWarnings("unchecked")
      Long start = (Long) flat.get("start");
      assertTrue("Should be sorted by start",
          previousStart.compareTo(start) <= 0);
      previousStart = start;
      actualCount++;
    }

    assertEquals(expectedCount, actualCount);
  }

  static class HiddenFileFilter implements FileFilter {
    @Override
    public boolean accept(File pathname) {
      return !pathname.getName().startsWith(".") && !pathname.getName().startsWith("_");
    }
  }
}
