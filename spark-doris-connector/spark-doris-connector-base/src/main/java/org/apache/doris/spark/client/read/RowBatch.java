// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.spark.client.read;

import com.google.common.base.Preconditions;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.apache.doris.spark.exception.DorisException;
import org.apache.doris.spark.rest.models.Schema;
import org.apache.doris.spark.util.IPUtils;
import org.apache.spark.sql.types.Decimal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * row batch data container.
 */
public class RowBatch implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(RowBatch.class);
    private static final ZoneId DEFAULT_ZONE_ID = ZoneId.systemDefault();

    private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
            .toFormatter();

    private static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final String DATETIMEV2_PATTERN = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    private final DateTimeFormatter dateTimeFormatter =
            DateTimeFormatter.ofPattern(DATETIME_PATTERN);
    private final DateTimeFormatter dateTimeV2Formatter =
            DateTimeFormatter.ofPattern(DATETIMEV2_PATTERN);
    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private final List<Row> rowBatch = new ArrayList<>();
    private final ArrowReader arrowReader;
    private final Schema schema;
    private RootAllocator rootAllocator;
    // offset for iterate the rowBatch
    private int offsetInRowBatch = 0;
    private int rowCountInOneBatch = 0;
    private int readRowCount = 0;
    private List<FieldVector> fieldVectors;

    private final Boolean datetimeJava8ApiEnabled;

    public RowBatch(TScanBatchResult nextResult, Schema schema, Boolean datetimeJava8ApiEnabled) throws DorisException {

        this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        this.arrowReader = new ArrowStreamReader(new ByteArrayInputStream(nextResult.getRows()), rootAllocator);
        this.schema = schema;
        this.datetimeJava8ApiEnabled = datetimeJava8ApiEnabled;

        try {
            VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
            while (arrowReader.loadNextBatch()) {
                readBatch(root);
            }
        } catch (Exception e) {
            logger.error("Read Doris Data failed because: ", e);
            throw new DorisException(e.getMessage());
        } finally {
            close();
        }

    }

    public RowBatch(ArrowReader reader, Schema schema, Boolean datetimeJava8ApiEnabled) throws DorisException {

        this.arrowReader = reader;
        this.schema = schema;
        this.datetimeJava8ApiEnabled = datetimeJava8ApiEnabled;

        try {
            VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
            readBatch(root);
        } catch (Exception e) {
            logger.error("Read Doris Data failed because: ", e);
            throw new DorisException(e.getMessage());
        }

    }

    private void readBatch(VectorSchemaRoot root) throws DorisException {
        fieldVectors = root.getFieldVectors();
        if (fieldVectors.size() > schema.size()) {
            logger.error("Data schema size '{}' should not be bigger than arrow field size '{}'.",
                    schema.size(), fieldVectors.size());
            throw new DorisException("Load Doris data failed, schema size of fetch data is wrong.");
        }
        if (fieldVectors.isEmpty() || root.getRowCount() == 0) {
            logger.debug("One batch in arrow has no data.");
            return;
        }
        rowCountInOneBatch = root.getRowCount();
        // init the rowBatch
        for (int i = 0; i < rowCountInOneBatch; ++i) {
            rowBatch.add(new Row(fieldVectors.size()));
        }
        convertArrowToRowBatch();
        readRowCount += root.getRowCount();
    }

    public static LocalDateTime longToLocalDateTime(long time) {
        Instant instant;
        // Determine the timestamp accuracy and process it
        if (time < 10_000_000_000L) { // Second timestamp
            instant = Instant.ofEpochSecond(time);
        } else if (time < 10_000_000_000_000L) { // milli second
            instant = Instant.ofEpochMilli(time);
        } else { // micro second
            instant = Instant.ofEpochSecond(time / 1_000_000, (time % 1_000_000) * 1_000);
        }
        return LocalDateTime.ofInstant(instant, DEFAULT_ZONE_ID);
    }

    public boolean hasNext() {
        if (offsetInRowBatch >= readRowCount) {
            rowBatch.clear();
            return false;
        }
        return true;
    }

    private void addValueToRow(int rowIndex, Object obj) {
        if (rowIndex > rowCountInOneBatch) {
            String errMsg = "Get row offset: " + rowIndex + " larger than row size: " + rowCountInOneBatch;
            logger.error(errMsg);
            throw new NoSuchElementException(errMsg);
        }
        rowBatch.get(readRowCount + rowIndex).put(obj);
    }

    public void convertArrowToRowBatch() throws DorisException {
        try {
            for (int col = 0; col < fieldVectors.size(); col++) {
                FieldVector curFieldVector = fieldVectors.get(col);
                MinorType mt = curFieldVector.getMinorType();

                final String colName = schema.get(col).getName();
                final String currentType = schema.get(col).getType();
                switch (currentType) {
                    case "NULL_TYPE":
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            addValueToRow(rowIndex, null);
                        }
                        break;
                    case "BOOLEAN":
                        Preconditions.checkArgument(mt.equals(MinorType.BIT),
                                typeMismatchMessage(colName, currentType, mt));
                        BitVector bitVector = (BitVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = bitVector.isNull(rowIndex) ? null : bitVector.get(rowIndex) != 0;
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "TINYINT":
                        Preconditions.checkArgument(mt.equals(MinorType.TINYINT),
                                typeMismatchMessage(colName, currentType, mt));
                        TinyIntVector tinyIntVector = (TinyIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = tinyIntVector.isNull(rowIndex) ? null : tinyIntVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "SMALLINT":
                        Preconditions.checkArgument(mt.equals(MinorType.SMALLINT),
                                typeMismatchMessage(colName, currentType, mt));
                        SmallIntVector smallIntVector = (SmallIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = smallIntVector.isNull(rowIndex) ? null : smallIntVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "INT":
                        Preconditions.checkArgument(mt.equals(MinorType.INT),
                                typeMismatchMessage(colName, currentType, mt));
                        IntVector intVector = (IntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = intVector.isNull(rowIndex) ? null : intVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "BIGINT":
                        Preconditions.checkArgument(mt.equals(MinorType.BIGINT),
                                typeMismatchMessage(colName, currentType, mt));
                        BigIntVector bigIntVector = (BigIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "LARGEINT":
                        Preconditions.checkArgument(mt.equals(MinorType.FIXEDSIZEBINARY) ||
                                mt.equals(MinorType.VARCHAR), typeMismatchMessage(colName, currentType, mt));
                        if (mt.equals(MinorType.FIXEDSIZEBINARY)) {
                            FixedSizeBinaryVector largeIntVector = (FixedSizeBinaryVector) curFieldVector;
                            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                                if (largeIntVector.isNull(rowIndex)) {
                                    addValueToRow(rowIndex, null);
                                    continue;
                                }
                                byte[] bytes = largeIntVector.get(rowIndex);
                                ArrayUtils.reverse(bytes);
                                BigInteger largeInt = new BigInteger(bytes);
                                addValueToRow(rowIndex, Decimal.apply(largeInt));
                            }
                        } else {
                            VarCharVector largeIntVector = (VarCharVector) curFieldVector;
                            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                                if (largeIntVector.isNull(rowIndex)) {
                                    addValueToRow(rowIndex, null);
                                    continue;
                                }
                                String stringValue = new String(largeIntVector.get(rowIndex));
                                BigInteger largeInt = new BigInteger(stringValue);
                                addValueToRow(rowIndex, Decimal.apply(largeInt));
                            }
                        }
                        break;
                    case "IPV4":
                        Preconditions.checkArgument(mt.equals(MinorType.UINT4) || mt.equals(MinorType.INT),
                                typeMismatchMessage(colName, currentType, mt));
                        BaseIntVector ipv4Vector;
                        if (mt.equals(MinorType.INT)) {
                            ipv4Vector = (IntVector) curFieldVector;
                        } else {
                            ipv4Vector = (UInt4Vector) curFieldVector;
                        }
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = ipv4Vector.isNull(rowIndex) ? null :
                                    IPUtils.convertLongToIPv4Address(ipv4Vector.getValueAsLong(rowIndex));
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "FLOAT":
                        Preconditions.checkArgument(mt.equals(MinorType.FLOAT4),
                                typeMismatchMessage(colName, currentType, mt));
                        Float4Vector float4Vector = (Float4Vector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = float4Vector.isNull(rowIndex) ? null : float4Vector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "TIME":
                    case "DOUBLE":
                        Preconditions.checkArgument(mt.equals(MinorType.FLOAT8),
                                typeMismatchMessage(colName, currentType, mt));
                        Float8Vector float8Vector = (Float8Vector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = float8Vector.isNull(rowIndex) ? null : float8Vector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "BINARY":
                        Preconditions.checkArgument(mt.equals(MinorType.VARBINARY),
                                typeMismatchMessage(colName, currentType, mt));
                        VarBinaryVector varBinaryVector = (VarBinaryVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = varBinaryVector.isNull(rowIndex) ? null : varBinaryVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "DECIMAL":
                        Preconditions.checkArgument(mt.equals(MinorType.VARCHAR),
                                typeMismatchMessage(colName, currentType, mt));
                        VarCharVector varCharVectorForDecimal = (VarCharVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (varCharVectorForDecimal.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                continue;
                            }
                            String decimalValue = new String(varCharVectorForDecimal.get(rowIndex));
                            Decimal decimal = new Decimal();
                            try {
                                decimal.set(new scala.math.BigDecimal(new BigDecimal(decimalValue)));
                            } catch (NumberFormatException e) {
                                String errMsg = "Decimal response result '" + decimalValue + "' is illegal.";
                                logger.error(errMsg, e);
                                throw new DorisException(errMsg);
                            }
                            addValueToRow(rowIndex, decimal);
                        }
                        break;
                    case "DECIMALV2":
                    case "DECIMAL32":
                    case "DECIMAL64":
                    case "DECIMAL128I":
                        Preconditions.checkArgument(mt.equals(MinorType.DECIMAL),
                                typeMismatchMessage(colName, currentType, mt));
                        DecimalVector decimalVector = (DecimalVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (decimalVector.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                continue;
                            }
                            Decimal decimalV2 = Decimal.apply(decimalVector.getObject(rowIndex));
                            addValueToRow(rowIndex, decimalV2);
                        }
                        break;
                    case "DATE":
                    case "DATEV2":
                        Preconditions.checkArgument(mt.equals(MinorType.VARCHAR)
                                || mt.equals(MinorType.DATEDAY), typeMismatchMessage(colName, currentType, mt));
                        if (mt.equals(MinorType.VARCHAR)) {
                            VarCharVector date = (VarCharVector) curFieldVector;
                            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                                if (date.isNull(rowIndex)) {
                                    addValueToRow(rowIndex, null);
                                    continue;
                                }
                                String stringValue = new String(date.get(rowIndex));
                                LocalDate localDate = LocalDate.parse(stringValue);
                                if (datetimeJava8ApiEnabled) {
                                    addValueToRow(rowIndex, localDate);
                                } else {
                                    addValueToRow(rowIndex, Date.valueOf(localDate));
                                }
                            }
                        } else {
                            DateDayVector date = (DateDayVector) curFieldVector;
                            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                                if (date.isNull(rowIndex)) {
                                    addValueToRow(rowIndex, null);
                                    continue;
                                }
                                LocalDate localDate = LocalDate.ofEpochDay(date.get(rowIndex));
                                if (datetimeJava8ApiEnabled) {
                                    addValueToRow(rowIndex, localDate);
                                } else {
                                    addValueToRow(rowIndex, Date.valueOf(localDate));
                                }
                            }
                        }
                        break;
                    case "DATETIME":
                    case "DATETIMEV2":

                        if (mt.equals(MinorType.VARCHAR)) {
                            VarCharVector varCharVector = (VarCharVector) curFieldVector;
                            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                                if (varCharVector.isNull(rowIndex)) {
                                    addValueToRow(rowIndex, null);
                                    continue;
                                }
                                String value = new String(varCharVector.get(rowIndex), StandardCharsets.UTF_8);
                                addValueToRow(rowIndex, value);
                            }
                        } else if (curFieldVector instanceof TimeStampVector) {
                            TimeStampVector timeStampVector = (TimeStampVector) curFieldVector;
                            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                                if (timeStampVector.isNull(rowIndex)) {
                                    addValueToRow(rowIndex, null);
                                    continue;
                                }
                                LocalDateTime dateTime = getDateTime(rowIndex, timeStampVector);
                                String formatted = DATE_TIME_FORMATTER.format(dateTime);
                                addValueToRow(rowIndex, formatted);
                            }
                        } else {
                            String errMsg = String.format("Unsupported type for DATETIMEV2, minorType %s, class is %s",
                                    mt.name(), curFieldVector.getClass());
                            throw new java.lang.IllegalArgumentException(errMsg);
                        }
                        break;
                    case "CHAR":
                    case "VARCHAR":
                    case "STRING":
                    case "JSONB":
                    case "VARIANT":
                        Preconditions.checkArgument(mt.equals(MinorType.VARCHAR),
                                typeMismatchMessage(colName, currentType, mt));
                        VarCharVector varCharVector = (VarCharVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (varCharVector.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                continue;
                            }
                            String value = new String(varCharVector.get(rowIndex), StandardCharsets.UTF_8);
                            addValueToRow(rowIndex, value);
                        }
                        break;
                    case "IPV6":
                        Preconditions.checkArgument(mt.equals(MinorType.VARCHAR),
                                typeMismatchMessage(colName, currentType, mt));
                        VarCharVector ipv6VarcharVector = (VarCharVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (ipv6VarcharVector.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                break;
                            }
                            String ipv6Str = new String(ipv6VarcharVector.get(rowIndex));
                            String ipv6Address = IPUtils.fromBigInteger(new BigInteger(ipv6Str));
                            addValueToRow(rowIndex, ipv6Address);
                        }
                        break;
                    case "ARRAY":
                        Preconditions.checkArgument(mt.equals(MinorType.LIST),
                                typeMismatchMessage(colName, currentType, mt));
                        ListVector listVector = (ListVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (listVector.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                continue;
                            }
                            String value = listVector.getObject(rowIndex).toString();
                            addValueToRow(rowIndex, value);
                        }
                        break;
                    case "MAP":
                        Preconditions.checkArgument(mt.equals(MinorType.MAP),
                                typeMismatchMessage(colName, currentType, mt));
                        MapVector mapVector = (MapVector) curFieldVector;
                        UnionMapReader reader = mapVector.getReader();
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (mapVector.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                continue;
                            }
                            reader.setPosition(rowIndex);
                            Map<String, String> value = new HashMap<>();
                            while (reader.next()) {
                                value.put(Objects.toString(reader.key().readObject(), null),
                                        Objects.toString(reader.value().readObject(), null));
                            }
                            addValueToRow(rowIndex, JavaConverters.mapAsScalaMapConverter(value).asScala());
                        }
                        break;
                    case "STRUCT":
                        Preconditions.checkArgument(mt.equals(MinorType.STRUCT),
                                typeMismatchMessage(colName, currentType, mt));
                        StructVector structVector = (StructVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (structVector.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                continue;
                            }
                            String value = structVector.getObject(rowIndex).toString();
                            addValueToRow(rowIndex, value);
                        }
                        break;
                    default:
                        String errMsg = "Unsupported type " + schema.get(col).getType() + "of col: " + schema.get(col).getName();
                        logger.error(errMsg);
                        throw new DorisException(errMsg);
                }
            }
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    public List<Object> next() {
        if (!hasNext()) {
            String errMsg = "Get row offset:" + offsetInRowBatch + " larger than row size: " + readRowCount;
            logger.error(errMsg);
            throw new NoSuchElementException(errMsg);
        }
        return rowBatch.get(offsetInRowBatch++).getCols();
    }

    private String typeMismatchMessage(final String columnName, final String sparkType, final MinorType arrowType) {
        final String messageTemplate = "Spark type for column %1$s is %2$s, but arrow type is %3$s.";
        return String.format(messageTemplate, columnName, sparkType, arrowType.name());
    }

    public int getReadRowCount() {
        return readRowCount;
    }

    public void close() {
        try {
            if (arrowReader != null) {
                arrowReader.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
        } catch (IOException ioe) {
            // do nothing
        }
    }

    public LocalDateTime getDateTime(int rowIndex, FieldVector fieldVector) {
        TimeStampVector vector = (TimeStampVector) fieldVector;
        if (vector.isNull(rowIndex)) {
            return null;
        }
        // todo: Currently, the scale of doris's arrow datetimev2 is hardcoded to 6,
        // and there is also a time zone problem in arrow, so use timestamp to convert first
        long time = vector.get(rowIndex);
        return longToLocalDateTime(time);
    }

    public static String completeMilliseconds(String stringValue) {
        if (stringValue.length() == DATETIMEV2_PATTERN.length()) {
            return stringValue;
        }

        if (stringValue.length() < DATETIME_PATTERN.length()) {
            return stringValue;
        }

        StringBuilder sb = new StringBuilder(stringValue);
        if (stringValue.length() == DATETIME_PATTERN.length()) {
            sb.append(".");
        }
        while (sb.toString().length() < DATETIMEV2_PATTERN.length()) {
            sb.append(0);
        }
        return sb.toString();
    }

    public static class Row {
        private final List<Object> cols;

        Row(int colCount) {
            this.cols = new ArrayList<>(colCount);
        }

        List<Object> getCols() {
            return cols;
        }

        public void put(Object o) {
            cols.add(o);
        }
    }
}
