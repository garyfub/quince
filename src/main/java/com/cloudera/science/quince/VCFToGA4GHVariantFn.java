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

import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.ga4gh.models.CallSet;
import org.ga4gh.models.Variant;
import org.ga4gh.models.VariantSet;
import org.opencb.hpg.bigdata.core.converters.FullVcfCodec;
import org.opencb.hpg.bigdata.core.converters.variation.Genotype2CallSet;
import org.opencb.hpg.bigdata.core.converters.variation.VariantContext2VariantConverter;
import org.opencb.hpg.bigdata.core.converters.variation.VariantConverterContext;
import org.opencb.hpg.bigdata.core.io.VcfBlockIterator;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import scala.Tuple2;

public class VCFToGA4GHVariantFn implements
    Function<Tuple2<LongWritable, VariantContextWritable>, Variant> {

  private Broadcast<VariantContext2VariantConverter> converterBroadcast;

  public VCFToGA4GHVariantFn(Broadcast<VariantContext2VariantConverter>
      converterBroadcast) {
    this.converterBroadcast = converterBroadcast;
  }

  static VariantContext2VariantConverter buildConverter(Configuration conf, Path[] vcfs,
      String sampleGroup) throws IOException {
    VariantContext2VariantConverter converter = new VariantContext2VariantConverter();
    VariantConverterContext variantConverterContext = new VariantConverterContext();

    for (Path vcf : vcfs) {
      InputStream inputStream = vcf.getFileSystem(conf).open(vcf);
      VcfBlockIterator iterator = new VcfBlockIterator(inputStream, new FullVcfCodec());
      VCFHeader header = iterator.getHeader();
      VariantSet vs = new VariantSet();
      vs.setId(vcf.getName());
      vs.setDatasetId(sampleGroup); // dataset = sample group
      VCFHeaderLine reference = header.getMetaDataLine("reference");
      vs.setReferenceSetId(reference == null ? "unknown" : reference.getValue());

      Genotype2CallSet gtConverter = new Genotype2CallSet();
      for (String genotypeSample : header.getGenotypeSamples()) {
        CallSet cs = gtConverter.forward(genotypeSample);
        cs.getVariantSetIds().add(vs.getId());
        // it's OK if there are duplicate sample names from different headers since the
        // only information we require for conversion is the name itself so there's no
        // scope for conflict
        variantConverterContext.getCallSetMap().put(cs.getName(), cs);
      }
    }
    converter.setContext(variantConverterContext);
    return converter;
  }

  @Override
  public Variant call(Tuple2<LongWritable, VariantContextWritable> input) throws Exception {
    return converterBroadcast.getValue().forward(input._2().get());
  }
}
