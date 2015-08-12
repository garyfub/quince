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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.SecondarySort;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.DataModelUtil;
import org.kitesdk.data.spi.EntityAccessor;
import org.kitesdk.data.spi.PartitionStrategyParser;
import org.kitesdk.data.spi.SchemaUtil;
import org.kitesdk.data.spi.StorageKey;

/*
 * Remove this class when https://issues.cloudera.org/browse/KITE-1032 is released.
 */
public final class CrunchDatasetsExtension {

  private CrunchDatasetsExtension() {
  }

  public static <E, K> PCollection<E> partitionAndSort(PCollection<E> collection,
      View<E> view, MapFn<E, GenericData.Record> getSortKeyFn, Schema sortKeySchema) {
    return partitionAndSort(collection, view, getSortKeyFn, sortKeySchema, -1);
  }

  public static <E, K> PCollection<E> partitionAndSort(PCollection<E> collection,
      View<E> view, MapFn<E, GenericData.Record> getSortKeyFn, Schema sortKeySchema,
      int numWriters) {
    return partitionAndSort(collection, view, getSortKeyFn, sortKeySchema, numWriters, 1);
  }

  public static <E, K> PCollection<E> partitionAndSort(PCollection<E> collection,
      View<E> view, MapFn<E, GenericData.Record> getSortKeyFn, Schema sortKeySchema,
      int numWriters, int numPartitionWriters) {
    //ensure the number of writers is honored whether it is per partition or total.
    DatasetDescriptor descriptor = view.getDataset().getDescriptor();
    if (descriptor.isPartitioned()) {
      GetStorageKey<E> getKey = new GetStorageKey<E>(view, numPartitionWriters);
      PTable<Pair<GenericData.Record, Integer>, Pair<GenericData.Record, E>> table =
          collection.parallelDo(new ExtractKeysFn<E>(getKey, getSortKeyFn),
              Avros.tableOf(
                  Avros.pairs(Avros.generics(getKey.schema()), Avros.ints()),
                  Avros.pairs(Avros.generics(sortKeySchema), collection.getPType())));
      @SuppressWarnings("unchecked")
      PCollection<E> sorted = SecondarySort.sortAndApply(table, new ExtractEntityFn(),
          collection.getPType(), numWriters);
      return sorted;
    } else {
      return partition(collection, numWriters);
    }
  }

  private static <E> PCollection<E> partition(PCollection<E> collection,
      int numReducers) {
    PType<E> type = collection.getPType();
    PTableType<E, Void> tableType = Avros.tableOf(type, Avros.nulls());
    PTable<E, Void> table = collection.parallelDo(new AsKeyTable<E>(), tableType);
    PGroupedTable<E, Void> grouped =
        numReducers > 0 ? table.groupByKey(numReducers) : table.groupByKey();
    return grouped.ungroup().keys();
  }

  private static class AsKeyTable<E> extends DoFn<E, Pair<E, Void>> {
    @Override
    public void process(E entity, Emitter<Pair<E, Void>> emitter) {
      emitter.emit(Pair.of(entity, (Void) null));
    }
  }

  private static final class GetStorageKey<E>
      extends MapFn<E, Pair<GenericData.Record, Integer>> {
    private final String strategyString;
    private final String schemaString;
    private final Class<E> type;
    private final Map<String, String> constraints;
    private final int numPartitionWriters;
    private transient AvroStorageKey key = null;
    private transient EntityAccessor<E> accessor = null;
    private transient Map<String, Object> provided = null;
    private transient int count;

    private GetStorageKey(View<E> view, int numPartitionWriters) {
      DatasetDescriptor descriptor = view.getDataset().getDescriptor();
      // get serializable versions of transient objects
      this.strategyString = descriptor.getPartitionStrategy()
          .toString(false /* no white space */);
      this.schemaString = descriptor.getSchema()
          .toString(false /* no white space */);
      this.type = view.getType();
      if (view instanceof AbstractRefinableView) {
        this.constraints = ((AbstractRefinableView) view).getConstraints()
            .toQueryMap();
      } else {
        this.constraints = null;
      }
      this.numPartitionWriters = numPartitionWriters > 0 ? numPartitionWriters : 1;
    }

    public Schema schema() {
      initialize(); // make sure the key is not null
      return key.getSchema();
    }

    @Override
    public void initialize() {
      if (key == null) {
        // restore transient objects from serializable versions
        PartitionStrategy strategy = PartitionStrategyParser.parse(strategyString);
        Schema schema = new Schema.Parser().parse(schemaString);
        this.key = new AvroStorageKey(strategy, schema);
        this.accessor = DataModelUtil.accessor(type, schema);
        if (constraints != null) {
          this.provided = Constraints
              .fromQueryMap(schema, strategy, constraints)
              .getProvidedValues();
        }
      }
      count = 0;
    }

    @Override
    public Pair<GenericData.Record, Integer> map(E entity) {
      int marker = count % numPartitionWriters;
      count += 1;
      return Pair.<GenericData.Record, Integer>of(key.reuseFor(entity, provided, accessor), marker);
    }
  }

  private static final class ExtractKeysFn<E>
      extends DoFn<E, Pair<Pair<GenericData.Record, Integer>, Pair<GenericData.Record, E>>> {

    private GetStorageKey<E> getStorageKey;
    private final MapFn<E, GenericData.Record> getSortKey;

    private ExtractKeysFn(GetStorageKey<E> getStorageKey, MapFn<E, GenericData.Record> getSortKey) {
      this.getStorageKey = getStorageKey;
      this.getSortKey = getSortKey;
    }

    @Override
    public void initialize() {
      getStorageKey.initialize();
      getSortKey.initialize();
    }

    @Override
    public void process(E entity,
        Emitter<Pair<Pair<GenericData.Record, Integer>, Pair<GenericData.Record, E>>> emitter) {
      Pair<GenericData.Record, Integer> storageKey = getStorageKey.map(entity);
      GenericData.Record sortKey = getSortKey.map(entity);
      emitter.emit(Pair.of(storageKey, Pair.of(sortKey, entity)));
    }
  }

  private static class ExtractEntityFn<E>
      extends DoFn<Pair<GenericData.Record, Iterable<Pair<GenericData.Record, E>>>, E> {
    @Override
    public void process(Pair<GenericData.Record,
        Iterable<Pair<GenericData.Record, E>>> input, Emitter<E> emitter) {
      for (Pair<GenericData.Record, E> pair : input.second()) {
        emitter.emit(pair.second());
      }
    }
  }

  private static final class AvroStorageKey extends GenericData.Record {
    private final StorageKey key;

    private AvroStorageKey(PartitionStrategy strategy, Schema schema) {
      super(SchemaUtil.keySchema(schema, strategy));
      this.key = new StorageKey(strategy);
    }

    public <E> AvroStorageKey reuseFor(E entity,
        @Nullable Map<String, Object> provided,
        EntityAccessor<E> accessor) {
      key.reuseFor(entity, provided, accessor);
      return this;
    }

    @Override
    public void put(int i, Object v) {
      key.replace(i, v);
    }

    @Override
    public Object get(int i) {
      return key.get(i);
    }
  }
}