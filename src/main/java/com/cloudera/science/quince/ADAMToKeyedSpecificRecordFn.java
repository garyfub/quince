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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Set;
import org.apache.avro.specific.SpecificRecord;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.bdgenomics.formats.avro.Genotype;
import org.bdgenomics.formats.avro.Variant;

import java.util.Collection;
import scala.Tuple2;
import scala.Tuple3;

/**
 * Extract the key from an ADAM {@link Variant} and optionally flatten the record and
 * expand genotype calls.
 */
public class ADAMToKeyedSpecificRecordFn
    implements PairFlatMapFunction<Tuple2<Variant, Collection<Genotype>>,
    Tuple3<String, Long, String>, SpecificRecord> {
  private boolean variantsOnly;
  private boolean flatten;
  private String sampleGroup;
  private Set<String> samples;

  public ADAMToKeyedSpecificRecordFn(boolean variantsOnly, boolean flatten,
      String sampleGroup, Set<String> samples) {
    this.variantsOnly = variantsOnly;
    this.flatten = flatten;
    this.sampleGroup = sampleGroup;
    this.samples = samples;
  }

  @Override
  public Iterable<Tuple2<Tuple3<String, Long, String>, SpecificRecord>>
    call(Tuple2<Variant, Collection<Genotype>> input) {
    Variant variant = input._1();
    String contig = variant.getContig().getContigName();
    long pos = variant.getStart();
    if (variantsOnly) {
      Tuple3<String, Long, String> key = new Tuple3<>(contig, pos, sampleGroup);
      SpecificRecord sr = flatten ? ADAMVariantFlattener.flattenVariant(variant) : variant;
      return ImmutableList.of(new Tuple2<>(key, sr));
    } else {  // genotype calls
      List<Tuple2<Tuple3<String, Long, String>, SpecificRecord>> tuples =
          Lists.newArrayList();
      for (Genotype genotype : input._2()) {
        if (samples == null || samples.contains(genotype.getSampleId())) {
          Tuple3<String, Long, String> key = new Tuple3<>(contig, pos, sampleGroup);
          SpecificRecord sr = flatten ? ADAMVariantFlattener.flattenGenotype(genotype) : genotype;
          tuples.add(new Tuple2<>(key, sr));
        }
      }
      return tuples;
    }
  }
}
