/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.dts.avro;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.tencent.subscribe.avro.Field;
import com.tencent.subscribe.avro.Record;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Tool class used to convert from Avro {@link Record} to {@link RowData}. * */
@Internal
public class AvroToRowDataConverters {

    /**
     * Runtime converter that converts Avro data structures into objects of Flink Table & SQL
     * internal data structures.
     */
    @FunctionalInterface
    public interface AvroToRowDataConverter extends Serializable {
        Object convert(Object object);
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    public static AvroToRowDataConverter createRowConverter(RowType rowType) {
        final Map<String, AvroToRowDataConverter> fieldConverters = rowType.getFields().stream()
                .collect(Collectors.toMap(
                        RowType.RowField::getName,
                        field -> AvroToRowDataConverters.createNullableConverter(field.getType())
                ));

        return avroObject -> {
            Record record = (Record) avroObject;
            List<Field> columns = record.getColumns();

            GenericRowData row = new GenericRowData(2);
            if (record.getOldColumns() != null) {
                List before = (List) record.getOldColumns();
                row.setField(0, generateRowData(rowType, fieldConverters, columns, before));
            }
            if (record.getNewColumns() != null) {
                List after = (List) record.getNewColumns();
                row.setField(1, generateRowData(rowType, fieldConverters, columns, after));
            }
            return row;
        };
    }

    private static GenericRowData generateRowData(
            RowType rowType,
            Map<String, AvroToRowDataConverter> fieldConverters,
            List<Field> columns,
            List<Object> values) {
        GenericRowData row = new GenericRowData(rowType.getFieldCount());

        Map<String, Integer> columnIndexMap = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            Field column = columns.get(i);
            if (column != null && column.getName() != null) {
                columnIndexMap.put(column.getName().toString(), i);
            }
        }

        List<String> fieldNames = rowType.getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);

            AvroToRowDataConverter converter = fieldConverters.get(fieldName);
            if (converter == null) {
                throw new IllegalArgumentException("Missing converter for field: " + fieldName);
            }

            Object rawValue = null;
            Integer columnIndex = columnIndexMap.get(fieldName);
            if (columnIndex != null && columnIndex < values.size()) {
                rawValue = values.get(columnIndex);
            }

            row.setField(i, converter.convert(rawValue));
        }
        return row;
    }

    /** Creates a runtime converter which is null safe. */
    private static AvroToRowDataConverter createNullableConverter(LogicalType type) {
        final AvroToRowDataConverter converter = createConverter(type);
        return avroObject -> {
            if (avroObject == null) {
                return null;
            }
            return converter.convert(avroObject);
        };
    }

    /** Creates a runtime converter which assuming input object is not null. */
    private static AvroToRowDataConverter createConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return avroObject -> null;
            case TINYINT:
                return avroObject -> Integer
                        .valueOf(((com.tencent.subscribe.avro.Integer) avroObject)
                                .getValue()
                                .toString())
                        .byteValue();
            case SMALLINT:
                return avroObject -> Integer
                        .valueOf(((com.tencent.subscribe.avro.Integer) avroObject)
                                .getValue()
                                .toString())
                        .shortValue();
            case BOOLEAN: // boolean
                return avroObject -> Boolean.valueOf(((com.tencent.subscribe.avro.Integer) avroObject)
                        .getValue()
                        .toString());
            case INTEGER: // int
                return avroObject -> Integer.valueOf(((com.tencent.subscribe.avro.Integer) avroObject)
                        .getValue()
                        .toString());
            case INTERVAL_YEAR_MONTH: // long
            case BIGINT: // long
            case INTERVAL_DAY_TIME: // long
                return avroObject -> Long.valueOf(((com.tencent.subscribe.avro.Integer) avroObject)
                        .getValue()
                        .toString());
            case FLOAT: // float
                return avroObject -> Double
                        .valueOf(((com.tencent.subscribe.avro.Float) avroObject).getValue())
                        .floatValue();
            case DOUBLE: // double
                return avroObject -> ((com.tencent.subscribe.avro.Float) avroObject).getValue();
            case DATE:
                return AvroToRowDataConverters::convertToDate;
            case TIME_WITHOUT_TIME_ZONE:
                return AvroToRowDataConverters::convertToTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return AvroToRowDataConverters::convertToTimestamp;
            case CHAR:
            case VARCHAR:
                return AvroToRowDataConverters::convertToString;
            case BINARY:
            case VARBINARY:
                return avroObject -> ((com.tencent.subscribe.avro.BinaryObject) avroObject)
                        .getValue()
                        .array();
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ROW:
                return createRowConverter((RowType) type);
            case ARRAY:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static AvroToRowDataConverter createDecimalConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return avroObject -> {
            com.tencent.subscribe.avro.Decimal data = (com.tencent.subscribe.avro.Decimal) avroObject;
            BigDecimal bigDecimal = new BigDecimal(data.getValue().toString());
            return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
        };
    }

    private static TimestampData convertToTimestamp(Object object) {
        TimestampData timestamp = null;
        if (object instanceof com.tencent.subscribe.avro.Timestamp) {
            com.tencent.subscribe.avro.Timestamp data = (com.tencent.subscribe.avro.Timestamp) object;
            long millis = data.getTimestamp() * 1000 + data.getMillis();
            if (millis > 0) {
                Instant instant = Instant.ofEpochMilli(millis);
                timestamp = TimestampData.fromLocalDateTime(instant.atZone(ZoneId.systemDefault()).toLocalDateTime());
            }
        } else if (object instanceof com.tencent.subscribe.avro.DateTime) {
            com.tencent.subscribe.avro.DateTime data = (com.tencent.subscribe.avro.DateTime) object;
            if (data.getYear() > 0 || data.getMonth() > 0 || data.getDay() > 0 || data.getHour() > 0
                    || data.getMinute() > 0 || data.getSecond() > 0) {
                LocalDateTime datetime = LocalDateTime.of(
                        data.getYear(), data.getMonth(),
                        data.getDay(),
                        data.getHour(),
                        data.getMinute(),
                        data.getSecond());
                timestamp = TimestampData.fromLocalDateTime(datetime);
            }
        } else {
            throw new IllegalArgumentException(
                    "Unexpected object type for TIMESTAMP logical type. Received: " + object);
        }
        return timestamp;
    }

    private static int convertToDate(Object object) {
        com.tencent.subscribe.avro.DateTime data = (com.tencent.subscribe.avro.DateTime) object;
        if (data.getYear() == 0 && data.getMonth() == 0 && data.getDay() == 0) {
            return 0;
        }
        return (int) LocalDate.of(data.getYear(), data.getMonth(), data.getDay()).toEpochDay();
    }

    private static int convertToTime(Object object) {
        com.tencent.subscribe.avro.DateTime data = (com.tencent.subscribe.avro.DateTime) object;
        if (data.getHour() == 0 && data.getMinute() == 0 && data.getSecond() == 0) {
            return 0;
        }
        return LocalTime.of(data.getHour(), data.getMinute(), data.getSecond()).toSecondOfDay()
                * 1000;
    }

    private static StringData convertToString(Object object) {
        String result = null;
        if (object instanceof com.tencent.subscribe.avro.Character) {
            com.tencent.subscribe.avro.Character data = (com.tencent.subscribe.avro.Character) object;
            byte[] bytes = data.getValue().array();
            String charset = data.getCharset().toString();
            result = new String(bytes, Charset.forName(charset));
        } else if (object instanceof com.tencent.subscribe.avro.TextObject) {
            com.tencent.subscribe.avro.TextObject data = (com.tencent.subscribe.avro.TextObject) object;
            result = data.getValue().toString();
        } else if (object instanceof com.tencent.subscribe.avro.Timestamp) {
            com.tencent.subscribe.avro.Timestamp data = (com.tencent.subscribe.avro.Timestamp) object;
            long millis = data.getTimestamp() * 1000 + data.getMillis();
            if (millis > 0) {
                result = String.valueOf(millis);
            }
        } else if (object instanceof com.tencent.subscribe.avro.DateTime) {
            com.tencent.subscribe.avro.DateTime data = (com.tencent.subscribe.avro.DateTime) object;
            if (data.getYear() > 0 || data.getMonth() > 0 || data.getDay() > 0 || data.getHour() > 0
                    || data.getMinute() > 0 || data.getSecond() > 0) {
                result = String.format(
                        "%04d-%02d-%02d %02d:%02d:%02d",
                        data.getYear(),
                        data.getMonth(),
                        data.getDay(),
                        data.getHour(),
                        data.getMinute(),
                        data.getSecond());
            }
        } else {
            throw new IllegalArgumentException(
                    "Unexpected object type for CHAR/VARCHAR logical type. Received: " + object);
        }
        return StringData.fromString(result);
    }
}
