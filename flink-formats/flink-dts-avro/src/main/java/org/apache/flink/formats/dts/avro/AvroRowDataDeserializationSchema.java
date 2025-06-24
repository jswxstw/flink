/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.dts.avro;

import com.tencent.subscribe.avro.Record;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;

/**
 * Deserialization schema from Avro bytes to {@link RowData}.
 *
 * <p>Deserializes the <code>byte[]</code> messages into (nested) Flink RowData. It converts Avro
 * types into types that are compatible with Flink's Table & SQL API.
 *
 * <p>Projects with Avro records containing logical date/time types need to add a JodaTime
 * dependency.
 */
@PublicEvolving
public class AvroRowDataDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    /**
     * Nested schema to deserialize the inputs into avro {@link Record}. *
     */
    private transient DatumReader<Record> reader;

    /**
     * Type information describing the result type.
     */
    private final TypeInformation<RowData> typeInfo;

    /**
     * Runtime instance that performs the actual work.
     */
    private final AvroToRowDataConverters.AvroToRowDataConverter runtimeConverter;

    /**
     * Creates a Avro deserialization schema for the given logical type.
     *
     * @param rowType  The logical type used to deserialize the data.
     * @param typeInfo The TypeInformation to be used by {@link
     *                 AvroRowDataDeserializationSchema#getProducedType()}.
     */
    public AvroRowDataDeserializationSchema(RowType rowType, TypeInformation<RowData> typeInfo) {
        this.typeInfo = typeInfo;
        this.runtimeConverter = AvroToRowDataConverters.createRowConverter(rowType);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        this.reader = new SpecificDatumReader<>(Record.class);
    }

    @Override
    public RowData deserialize(@Nullable byte[] message) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
        if (message == null || message.length == 0) {
            // skip tombstone messages
            return;
        }
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(message, null);
            Record record = reader.read(null, decoder);
            GenericRowData row = (GenericRowData) runtimeConverter.convert(record);
            GenericRowData before = (GenericRowData) row.getField(0);
            GenericRowData after = (GenericRowData) row.getField(1);
            switch (record.getMessageType()) {
                case INIT_INSERT:
                case INSERT:
                    after.setRowKind(RowKind.INSERT);
                    out.collect(after);
                    break;
                case UPDATE:
                    before.setRowKind(RowKind.UPDATE_BEFORE);
                    after.setRowKind(RowKind.UPDATE_AFTER);
                    out.collect(before);
                    out.collect(after);
                    break;
                case DELETE:
                    before.setRowKind(RowKind.DELETE);
                    out.collect(before);
                    break;
            }
        } catch (Exception e) {
            throw new IOException("Failed to deserialize Avro record.", e);
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return typeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AvroRowDataDeserializationSchema that = (AvroRowDataDeserializationSchema) o;
        return typeInfo.equals(that.typeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeInfo);
    }
}
