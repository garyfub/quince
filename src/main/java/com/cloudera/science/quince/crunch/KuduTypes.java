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

package com.cloudera.science.quince.crunch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.MapFn;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.ReflectionUtils;
import org.kududb.client.Operation;

public final class KuduTypes {

  public static PType<Operation> operations() {
    return Writables.derived(Operation.class,
        new MapInFn<Operation>(Operation.class, OperationSerialization.class),
        new MapOutFn<Operation>(Operation.class, OperationSerialization.class),
        Writables.bytes());
  }

  private static class MapInFn<T> extends MapFn<ByteBuffer, T> {
    private Class<T> clazz;
    private Class<? extends Serialization> serClazz;
    private transient Deserializer<T> deserializer;

    public MapInFn(Class<T> clazz, Class<? extends Serialization> serClazz) {
      this.clazz = clazz;
      this.serClazz = serClazz;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void initialize() {
      this.deserializer = ReflectionUtils.newInstance(serClazz, null).getDeserializer(clazz);
      if (deserializer == null) {
        throw new CrunchRuntimeException("No Hadoop deserializer for class: " + clazz);
      }
    }

    @Override
    public T map(ByteBuffer bb) {
      if (deserializer == null) {
        initialize();
      }
      ByteArrayInputStream bais = new ByteArrayInputStream(bb.array(), bb.position(), bb.limit());
      try {
        deserializer.open(bais);
        T ret = deserializer.deserialize(null);
        deserializer.close();
        return ret;
      } catch (Exception e) {
        throw new CrunchRuntimeException("Deserialization errror", e);
      }
    }
  }

  private static class MapOutFn<T> extends MapFn<T, ByteBuffer> {
    private Class<T> clazz;
    private Class<? extends Serialization> serClazz;
    private transient Serializer<T> serializer;

    public MapOutFn(Class<T> clazz, Class<? extends Serialization> serClazz) {
      this.clazz = clazz;
      this.serClazz = serClazz;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void initialize() {
      this.serializer = ReflectionUtils.newInstance(serClazz, null).getSerializer(clazz);
      if (serializer == null) {
        throw new CrunchRuntimeException("No Hadoop serializer for class: " + clazz);
      }
    }

    @Override
    public ByteBuffer map(T out) {
      if (serializer == null) {
        initialize();
      }
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        serializer.open(baos);
        serializer.serialize(out);
        serializer.close();
        return ByteBuffer.wrap(baos.toByteArray());
      } catch (Exception e) {
        throw new CrunchRuntimeException("Serialization errror", e);
      }
    }
  }

  private KuduTypes() {}
}
