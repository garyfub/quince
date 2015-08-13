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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.kududb.client.Operation;

public class OperationSerialization implements Serialization<Operation> {

  @Override
  public boolean accept(Class<?> c) {
    return Operation.class.isAssignableFrom(c);
  }

  @Override
  public Serializer<Operation> getSerializer(Class<Operation> c) {
    return new OperationSerializer();
  }

  @Override
  public Deserializer<Operation> getDeserializer(Class<Operation> c) {
    return new OperationDeserializer();
  }

  private static class OperationSerializer implements Serializer<Operation> {
    private OutputStream out;

    @Override
    public void open(OutputStream output) throws IOException {
      this.out = output;
    }

    @Override
    public void serialize(Operation operation) throws IOException {
      throw new UnsupportedOperationException("Serialization of Kudu operation types is" +
          " not supported.");
      // TODO: make Operation.createAndFillWriteRequestPB public
      //Operation.createAndFillWriteRequestPB(operation).build().writeDelimitedTo(out);
    }

    @Override
    public void close() throws IOException {
      out.close();
    }

  }

  private static class OperationDeserializer implements Deserializer<Operation> {
    private InputStream in;

    @Override
    public void open(InputStream input) throws IOException {
      this.in = input;
    }

    @Override
    public Operation deserialize(Operation operation) throws IOException {
      throw new UnsupportedOperationException("Serialization of Kudu operation types is" +
          " not supported.");
      // Tserver.WriteRequestPB writeRequestPB = Tserver.WriteRequestPB.parseDelimitedFrom(in);
      // TODO; write inverse of Operation.OperationsEncoder to decode a WriteRequestPB
      // into an Operation
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

  }
}
