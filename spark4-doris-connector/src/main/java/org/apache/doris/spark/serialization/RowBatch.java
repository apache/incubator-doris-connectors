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

package org.apache.doris.spark.serialization;

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
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
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

import static org.apache.doris.spark.util.IPUtils.convertLongToIPv4Address;

/**
 * row batch data container.
 */
public class RowBatch {
    private static final Logger logger = LoggerFactory.getLogger(RowBatch.class);

    private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
            .toFormatter();

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

    // offset for iterate the rowBatch
    private int offsetInRowBatch = 0;
    private int rowCountInOneBatch = 0;
    private int readRowCount = 0;
    private final List<Row> rowBatch = new ArrayList<>();
    private final ArrowStreamReader arrowStreamReader;
    private List<FieldVector> fieldVectors;
    private final RootAllocator rootAllocator;
    private final Schema schema;

    public RowBatch(TScanBatchResult nextResult, Schema schema) throws DorisException {
        this.schema = schema;
        this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        this.arrowStreamReader = new ArrowStreamReader(
                new ByteArrayInputStream(nextResult.getRows()),
                rootAllocator
        );
        try {
            VectorSchemaRoot root = arrowStreamReader.getVectorSchemaRoot();
            while (arrowStreamReader.loadNextBatch()) {
                fieldVectors = root.getFieldVectors();
                if (fieldVectors.size() > schema.size()) {
                    logger.error("Data schema size '{}' should not be bigger than arrow field size '{}'.",
                            schema.size(), fieldVectors.size());
                    throw new DorisException("Load Doris data failed, schema size of fetch data is wrong.");
                }
                if (fieldVectors.isEmpty() || root.getRowCount() == 0) {
                    logger.debug("One batch in arrow has no data.");
                    continue;
                }
                rowCountInOneBatch = root.getRowCount();
                // init the rowBatch
                for (int i = 0; i < rowCountInOneBatch; ++i) {
                    rowBatch.add(new Row(fieldVectors.size()));
                }
                convertArrowToRowBatch();
                readRowCount += root.getRowCount();
            }
        } catch (Exception e) {
            logger.error("Read Doris Data failed because: ", e);
            throw new DorisException(e.getMessage());
        } finally {
            close();
        }
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
            String errMsg = "Get row offset: " + rowIndex + " larger than row size: " +
                    rowCountInOneBatch;
            logger.error(errMsg);
            throw new NoSuchElementException(errMsg);
        }
        rowBatch.get(readRowCount + rowIndex).put(obj);
    }

    public void convertArrowToRowBatch() throws DorisException {
        try {
            for (int col = 0; col < fieldVectors.size(); col++) {
                FieldVector curFieldVector = fieldVectors.get(col);
                Types.MinorType mt = curFieldVector.getMinorType();

                final String currentType = schema.get(col).getType();
                switch (currentType) {
                    case "NULL_TYPE":
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            addValueToRow(rowIndex, null);
                        }
                        break;
                    case "BOOLEAN":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.BIT),
                                typeMismatchMessage(currentType, mt));
                        BitVector bitVector = (BitVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = bitVector.isNull(rowIndex) ? null : bitVector.get(rowIndex) != 0;
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "TINYINT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.TINYINT),
                                typeMismatchMessage(currentType, mt));
                        TinyIntVector tinyIntVector = (TinyIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = tinyIntVector.isNull(rowIndex) ? null : tinyIntVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "SMALLINT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.SMALLINT),
                                typeMismatchMessage(currentType, mt));
                        SmallIntVector smallIntVector = (SmallIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = smallIntVector.isNull(rowIndex) ? null : smallIntVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "INT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.INT),
                                typeMismatchMessage(currentType, mt));
                        IntVector intVector = (IntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = intVector.isNull(rowIndex) ? null : intVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "BIGINT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.BIGINT),
                                typeMismatchMessage(currentType, mt));
                        BigIntVector bigIntVector = (BigIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "LARGEINT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.FIXEDSIZEBINARY) ||
                                mt.equals(Types.MinorType.VARCHAR), typeMismatchMessage(currentType, mt));
                        if (mt.equals(Types.MinorType.FIXEDSIZEBINARY)) {
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
                        Preconditions.checkArgument(mt.equals(Types.MinorType.UINT4) || mt.equals(Types.MinorType.INT),
                                typeMismatchMessage(currentType, mt));
                        BaseIntVector ipv4Vector;
                        if (mt.equals(Types.MinorType.INT)) {
                            ipv4Vector = (IntVector) curFieldVector;
                        } else {
                            ipv4Vector = (UInt4Vector) curFieldVector;
                        }
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = ipv4Vector.isNull(rowIndex) ? null : convertLongToIPv4Address(ipv4Vector.getValueAsLong(rowIndex));
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "FLOAT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.FLOAT4),
                                typeMismatchMessage(currentType, mt));
                        Float4Vector float4Vector = (Float4Vector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = float4Vector.isNull(rowIndex) ? null : float4Vector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "TIME":
                    case "DOUBLE":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.FLOAT8),
                                typeMismatchMessage(currentType, mt));
                        Float8Vector float8Vector = (Float8Vector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = float8Vector.isNull(rowIndex) ? null : float8Vector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "BINARY":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.VARBINARY),
                                typeMismatchMessage(currentType, mt));
                        VarBinaryVector varBinaryVector = (VarBinaryVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = varBinaryVector.isNull(rowIndex) ? null : varBinaryVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "DECIMAL":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.VARCHAR),
                                typeMismatchMessage(currentType, mt));
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
                        Preconditions.checkArgument(mt.equals(Types.MinorType.DECIMAL),
                                typeMismatchMessage(currentType, mt));
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
                        Preconditions.checkArgument(mt.equals(Types.MinorType.VARCHAR)
                                || mt.equals(Types.MinorType.DATEDAY), typeMismatchMessage(currentType, mt));
                        if (mt.equals(Types.MinorType.VARCHAR)) {
                            VarCharVector date = (VarCharVector) curFieldVector;
                            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                                if (date.isNull(rowIndex)) {
                                    addValueToRow(rowIndex, null);
                                    continue;
                                }
                                String stringValue = new String(date.get(rowIndex));
                                LocalDate localDate = LocalDate.parse(stringValue);
                                addValueToRow(rowIndex, Date.valueOf(localDate));
                            }
                        } else {
                            DateDayVector date = (DateDayVector) curFieldVector;
                            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                                if (date.isNull(rowIndex)) {
                                    addValueToRow(rowIndex, null);
                                    continue;
                                }
                                LocalDate localDate = LocalDate.ofEpochDay(date.get(rowIndex));
                                addValueToRow(rowIndex, Date.valueOf(localDate));
                            }
                        }
                        break;
                    case "DATETIME":
                    case "DATETIMEV2":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.VARCHAR)
                                        || mt.equals(Types.MinorType.TIMESTAMPMICRO),
                                typeMismatchMessage(currentType, mt));
                        if (mt.equals(Types.MinorType.VARCHAR)) {
                            VarCharVector varCharVector = (VarCharVector) curFieldVector;
                            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                                if (varCharVector.isNull(rowIndex)) {
                                    addValueToRow(rowIndex, null);
                                    continue;
                                }
                                String value = new String(varCharVector.get(rowIndex), StandardCharsets.UTF_8);
                                addValueToRow(rowIndex, value);
                            }
                        } else {
                            TimeStampMicroVector vector = (TimeStampMicroVector) curFieldVector;
                            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                                if (vector.isNull(rowIndex)) {
                                    addValueToRow(rowIndex, null);
                                    continue;
                                }
                                long time = vector.get(rowIndex);
                                Instant instant;
                                if (time / 10000000000L == 0) { // datetime(0)
                                    instant = Instant.ofEpochSecond(time);
                                } else if (time / 10000000000000L == 0) { // datetime(3)
                                    instant = Instant.ofEpochMilli(time);
                                } else { // datetime(6)
                                    instant = Instant.ofEpochSecond(time / 1000000, time % 1000000 * 1000);
                                }
                                LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
                                String formatted = DATE_TIME_FORMATTER.format(dateTime);
                                addValueToRow(rowIndex, formatted);
                            }
                        }
                        break;
                    case "CHAR":
                    case "VARCHAR":
                    case "STRING":
                    case "JSONB":
                    case "VARIANT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.VARCHAR),
                                typeMismatchMessage(currentType, mt));
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
                        Preconditions.checkArgument(mt.equals(Types.MinorType.VARCHAR),
                                typeMismatchMessage(currentType, mt));
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
                        Preconditions.checkArgument(mt.equals(Types.MinorType.LIST),
                                typeMismatchMessage(currentType, mt));
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
                        Preconditions.checkArgument(mt.equals(Types.MinorType.MAP),
                                typeMismatchMessage(currentType, mt));
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
                        Preconditions.checkArgument(mt.equals(Types.MinorType.STRUCT),
                                typeMismatchMessage(currentType, mt));
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
                        String errMsg = "Unsupported type " + schema.get(col).getType();
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

    private String typeMismatchMessage(final String sparkType, final Types.MinorType arrowType) {
        final String messageTemplate = "Spark type is %1$s, but arrow type is %2$s.";
        return String.format(messageTemplate, sparkType, arrowType.name());
    }

    public int getReadRowCount() {
        return readRowCount;
    }

    public void close() {
        try {
            if (arrowStreamReader != null) {
                arrowStreamReader.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
        } catch (IOException ioe) {
            // do nothing
        }
    }
}
