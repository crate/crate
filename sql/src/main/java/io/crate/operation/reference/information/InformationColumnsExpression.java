/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.reference.information;

import io.crate.metadata.RowContextCollectorExpression;
import io.crate.types.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.MapBuilder;

import java.util.Map;


public abstract class InformationColumnsExpression<T>
    extends RowContextCollectorExpression<ColumnContext, T> {

    private final static Integer NUMERIC_PRECISION_RADIX = 2; // Binary
    private final static Integer DATETIME_PRECISION = 3; // Milliseconds

    /**
     * For fixed point numbers one bit is used for the sign.
     * Thus the precision for a ByteType is 8-1 bit with a range
     * from -128 to 127.
     *
     * For floating point numbers please refer to:
     * https://en.wikipedia.org/wiki/IEEE_floating_point
     */
    private static final Map<Integer, Integer> NUMERIC_PRECISION = new MapBuilder<Integer, Integer>()
        .put(ByteType.ID, 7)
        .put(ShortType.ID, 15)
        .put(FloatType.ID, 24)
        .put(IntegerType.ID, 31)
        .put(DoubleType.ID, 53)
        .put(LongType.ID, 63).map();

    public static class ColumnsSchemaNameExpression extends InformationColumnsExpression<BytesRef> {

        @Override
        public BytesRef value() {
            assert row.info.ident().tableIdent().schema() != null : "table schema can't be null";
            return new BytesRef(row.info.ident().tableIdent().schema());
        }
    }

    public static class ColumnsTableNameExpression extends InformationColumnsExpression<BytesRef> {

        @Override
        public BytesRef value() {
            assert row.info.ident().tableIdent().name() != null : "table name can't be null";
            return new BytesRef(row.info.ident().tableIdent().name());
        }
    }

    public static class ColumnsTableCatalogExpression extends InformationColumnsExpression<BytesRef> {

        @Override
        public BytesRef value() {
            assert row.info.ident().tableIdent().schema() != null : "table schema can't be null";
            return new BytesRef(row.info.ident().tableIdent().name());
        }
    }

    public static class ColumnsColumnNameExpression extends InformationColumnsExpression<BytesRef> {

        @Override
        public BytesRef value() {
            assert row.info.ident().tableIdent().name() != null : "column name name can't be null";
            return new BytesRef(row.info.ident().columnIdent().sqlFqn());
        }
    }

    public static class ColumnsNullExpression extends InformationColumnsExpression<BytesRef> {

        @Override
        public BytesRef value() {
            return null;
        }
    }

    public static class ColumnsOrdinalExpression extends InformationColumnsExpression<Short> {

        @Override
        public Short value() {
            return row.ordinal;
        }
    }

    public static class ColumnsDataTypeExpression extends InformationColumnsExpression<BytesRef> {

        @Override
        public BytesRef value() {
            assert row.info.valueType() != null && row.info.valueType().getName() !=
                                                   null : "columns must always have a type and the type must have a name";
            return new BytesRef(row.info.valueType().getName());
        }
    }

    public static class ColumnsIsNullableExpression extends InformationColumnsExpression<Boolean> {

        @Override
        public Boolean value() {
            if (row.tableInfo.primaryKey().contains(row.info.ident().columnIdent())) {
                return false;
            } else {
                return row.info.isNullable();
            }
        }
    }

    public static class ColumnsNumericPrecisionExpression extends InformationColumnsExpression<Integer> {

        @Override
        public Integer value() {
            assert row.info.valueType() != null && row.info.valueType().getName() !=
                                                   null : "columns must always have a type and the type must have a name";
            return NUMERIC_PRECISION.get(row.info.valueType().id());
        }
    }

    public static class ColumnsNumericPrecisionRadixExpression extends InformationColumnsExpression<Integer> {

        @Override
        public Integer value() {
            assert row.info.valueType() != null && row.info.valueType().getName() !=
                                                   null : "columns must always have a type and the type must have a name";
            if (DataTypes.NUMERIC_PRIMITIVE_TYPES.contains(row.info.valueType())) {
                return NUMERIC_PRECISION_RADIX;
            }
            return null;
        }
    }

    public static class ColumnsDatetimePrecisionExpression extends InformationColumnsExpression<Integer> {

        @Override
        public Integer value() {
            assert row.info.valueType() != null && row.info.valueType().getName() !=
                                                   null : "columns must always have a type and the type must have a name";
            if (row.info.valueType() == DataTypes.TIMESTAMP) {
                return DATETIME_PRECISION;
            }
            return null;
        }
    }
}
