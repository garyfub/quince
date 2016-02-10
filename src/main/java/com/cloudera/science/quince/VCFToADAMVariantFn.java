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

import htsjdk.variant.variantcontext.VariantContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.bdgenomics.adam.converters.VariantContextConverter;
import org.bdgenomics.adam.models.SequenceDictionary;
import org.bdgenomics.formats.avro.Genotype;
import org.bdgenomics.formats.avro.Variant;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;

public class VCFToADAMVariantFn implements
    PairFlatMapFunction<VariantContextWritable, Variant, Collection<Genotype>> {

  private VariantContextConverter vcc;

  public VCFToADAMVariantFn() {
    vcc = new VariantContextConverter(Option.<SequenceDictionary>apply(null));
  }

  @Override
  public Iterable<Tuple2<Variant, Collection<Genotype>>> call(VariantContextWritable input) {

    VariantContext bvc = input.get();
    List<org.bdgenomics.adam.models.VariantContext> avcList =
        JavaConversions.seqAsJavaList(vcc.convert(bvc));
    List<Tuple2<Variant, Collection<Genotype>>> pairs = new ArrayList<>();
    for (org.bdgenomics.adam.models.VariantContext avc : avcList) {
      Variant variant = avc.variant().variant();
      Collection<Genotype> genotypes = JavaConversions.asJavaCollection(avc.genotypes());
      pairs.add(new Tuple2<>(variant, genotypes));
    }
    return pairs;
  }
}
