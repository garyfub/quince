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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.avro.specific.SpecificRecord;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.ga4gh.models.Call;
import org.ga4gh.models.Variant;
import scala.Tuple2;
import scala.Tuple3;

/**
 * Extract the key from a GA4GH {@link Variant} and optionally flatten the record and
 * expand genotype calls.
 */
public class GA4GHToKeyedSpecificRecordFn
    implements PairFlatMapFunction<Variant, Tuple3<String, Long, String>,
        SpecificRecord> {

  private boolean variantsOnly;
  private boolean flatten;
  private String sampleGroup;
  private Set<String> samples;

  public GA4GHToKeyedSpecificRecordFn(boolean variantsOnly, boolean flatten,
      String sampleGroup, Set<String> samples) {
    this.variantsOnly = variantsOnly;
    this.flatten = flatten;
    this.sampleGroup = sampleGroup;
    this.samples = samples;
  }

  @Override
  public Iterable<Tuple2<Tuple3<String, Long, String>, SpecificRecord>> call(
      Variant input) {
    String contig = input.getReferenceName().toString();
    long pos = input.getStart();
    if (variantsOnly) {
      Tuple3<String, Long, String> key = new Tuple3<>(contig, pos, sampleGroup);
      SpecificRecord sr = flatten ? GA4GHVariantFlattener.flattenVariant(input) : input;
      return ImmutableList.of(new Tuple2<>(key, sr));
    } else {  // genotype calls
      Variant.Builder variantBuilder = Variant.newBuilder(input).clearCalls();
      List<Tuple2<Tuple3<String, Long, String>, SpecificRecord>> tuples =
          Lists.newArrayList();
      for (Call call : input.getCalls()) {
        if (samples == null || samples.contains(call.getCallSetId())) {
          Tuple3<String, Long, String> key = new Tuple3<>(contig, pos, sampleGroup);
          variantBuilder.setCalls(Collections.singletonList(call));
          Variant variant = variantBuilder.build();
          SpecificRecord sr = flatten ? GA4GHVariantFlattener.flattenCall(variant, call) : variant;
          tuples.add(new Tuple2<>(key, sr));
          variantBuilder.clearCalls();
        }
      }
      return tuples;
    }
  }
}
