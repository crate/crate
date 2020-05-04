/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.expression.reference.doc.lucene;

import io.crate.analyze.OrderBy;
import io.crate.types.BooleanType;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.TimeType;
import io.crate.types.TimestampType;

import org.apache.lucene.search.SortField;
import org.elasticsearch.index.fielddata.NullValueOrder;

import javax.annotation.Nullable;

public class NullSentinelValues {

    public static Object nullSentinelForScoreDoc(OrderBy orderBy, int orderIndex) {
        assert orderIndex <= orderBy.orderBySymbols().size() : "orderIndex must be < number of orderBy symbols";
        return nullSentinelForScoreDoc(
            orderBy.orderBySymbols().get(orderIndex).valueType(),
            orderBy.reverseFlags()[orderIndex],
            orderBy.nullsFirst()[orderIndex]
        );
    }

    public static Object nullSentinelForReducedType(SortField.Type reducedType,
                                                    NullValueOrder nullValueOrder,
                                                    boolean reversed) {
        final boolean min = nullValueOrder == NullValueOrder.FIRST ^ reversed;
        switch (reducedType) {
            case INT:
                return min ? Integer.MIN_VALUE : Integer.MAX_VALUE;
            case LONG:
                return min ? Long.MIN_VALUE : Long.MAX_VALUE;
            case FLOAT:
                return min ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
            case DOUBLE:
                return min ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
            case STRING:
            case STRING_VAL:
                return null;
            default:
                throw new UnsupportedOperationException("Unsupported reduced type: " + reducedType);
        }
    }

    /**
     * @return a sentinel value that is used to indicate a `null` value.
     *         The returned value here is **not** necessarily compatible with the value type of the given `type`.
     *         This can be used for in places where {@link org.apache.lucene.search.ScoreDoc} is used,
     *         which for example uses LONG also for columns of type INTEGER.
     */
    public static Object nullSentinelForScoreDoc(DataType<?> type, boolean reverseFlag, Boolean nullFirst) {
        boolean min = reverseFlag ^ (nullFirst != null ? nullFirst : reverseFlag);
        switch (type.id()) {
            case ByteType.ID:
            case ShortType.ID:
            case IntegerType.ID:
            case BooleanType.ID:
            case LongType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:
            case TimeType.ID:
                return min ? Long.MIN_VALUE : Long.MAX_VALUE;

            case FloatType.ID:
                return min ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;

            case DoubleType.ID:
                return min ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;

            default:
                return null;
        }
    }

    /**
     * Similar to {@link #nullSentinelForScoreDoc(DataType, boolean, Boolean)}
     * but returns a value that is compatible with the value type of the given DataType.
     */
    @Nullable
    public static Object nullSentinel(DataType<?> dataType, NullValueOrder nullValueOrder, boolean reversed) {
        boolean min = nullValueOrder == NullValueOrder.FIRST ^ reversed;
        switch (dataType.id()) {
            case ByteType.ID:
                return min ? Byte.MIN_VALUE : Byte.MAX_VALUE;

            case ShortType.ID:
                return min ? Short.MIN_VALUE : Short.MAX_VALUE;

            case IntegerType.ID:
            case TimeType.ID:
                return min ? Integer.MIN_VALUE : Integer.MAX_VALUE;

            case LongType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:
                return min ? Long.MIN_VALUE : Long.MAX_VALUE;

            case FloatType.ID:
                return min ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;

            case DoubleType.ID:
                return min ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;

            default:
                return null;
        }
    }
}
