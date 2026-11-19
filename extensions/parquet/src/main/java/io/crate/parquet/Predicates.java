/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.parquet;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;

import dev.hardwood.reader.FilterPredicate;
import io.crate.metadata.Reference;
import io.crate.types.BooleanType;
import io.crate.types.DataType;
import io.crate.types.DateType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.NumericType;
import io.crate.types.ShortType;
import io.crate.types.StringType;
import io.crate.types.TimestampType;
import io.crate.types.UUIDType;

public class Predicates {

    @FunctionalInterface
    interface FilterPredicateFactory<T> {
        // NOTE: The FilterPredicate methods (eq, gt, ...) match this interface's shape.
        // When a method reference like FilterPredicate::eq is passed as an argument, the
        // compiler chooses the overload that matches type T (for Integer, eq(String, int)).
        // The resulting object's create(...) calls that overload.
        // `create` is just a placeholder for the interface's method name; calling eq.create(fqn, v) runs
        // whatever method was passed in for eq.
        FilterPredicate create(String fqn, T value);
    }

    private static <T> FilterPredicate predicate(FilterPredicate.Operator op, String fqn, T v,
            FilterPredicateFactory<T> eq, FilterPredicateFactory<T> notEq,
            FilterPredicateFactory<T> lt, FilterPredicateFactory<T> ltEq,
            FilterPredicateFactory<T> gt, FilterPredicateFactory<T> gtEq) {
        return switch (op) {
            case EQ -> eq.create(fqn, v);
            case NOT_EQ -> notEq.create(fqn, v);
            case LT -> lt.create(fqn, v);
            case LT_EQ -> ltEq.create(fqn, v);
            case GT -> gt.create(fqn, v);
            case GT_EQ -> gtEq.create(fqn, v);
        };
    }

    private static FilterPredicate intPredicate(FilterPredicate.Operator op, String fqn, int v) {
        return predicate(op, fqn, v, FilterPredicate::eq, FilterPredicate::notEq, FilterPredicate::lt,
                FilterPredicate::ltEq, FilterPredicate::gt, FilterPredicate::gtEq);
    }

    private static FilterPredicate longPredicate(FilterPredicate.Operator op, String fqn, long v) {
        return predicate(op, fqn, v, FilterPredicate::eq, FilterPredicate::notEq, FilterPredicate::lt,
                FilterPredicate::ltEq, FilterPredicate::gt, FilterPredicate::gtEq);
    }

    private static FilterPredicate numericPredicate(FilterPredicate.Operator op, String fqn, BigDecimal v) {
        return predicate(op, fqn, v, FilterPredicate::eq, FilterPredicate::notEq, FilterPredicate::lt,
                FilterPredicate::ltEq, FilterPredicate::gt, FilterPredicate::gtEq);
    }

    private static FilterPredicate floatPredicate(FilterPredicate.Operator op, String fqn, Float v) {
        return predicate(op, fqn, v, FilterPredicate::eq, FilterPredicate::notEq, FilterPredicate::lt,
                FilterPredicate::ltEq, FilterPredicate::gt, FilterPredicate::gtEq);
    }

    private static FilterPredicate doublePredicate(FilterPredicate.Operator op, String fqn, Double v) {
        return predicate(op, fqn, v, FilterPredicate::eq, FilterPredicate::notEq, FilterPredicate::lt,
                FilterPredicate::ltEq, FilterPredicate::gt, FilterPredicate::gtEq);
    }

    private static FilterPredicate stringPredicate(FilterPredicate.Operator op, String fqn, String v) {
        return predicate(op, fqn, v, FilterPredicate::eq, FilterPredicate::notEq, FilterPredicate::lt,
                FilterPredicate::ltEq, FilterPredicate::gt, FilterPredicate::gtEq);
    }

    private static FilterPredicate datePredicate(FilterPredicate.Operator op, String fqn, LocalDate v) {
        return predicate(op, fqn, v, FilterPredicate::eq, FilterPredicate::notEq, FilterPredicate::lt,
                FilterPredicate::ltEq, FilterPredicate::gt, FilterPredicate::gtEq);
    }

    private static FilterPredicate timestampPredicate(FilterPredicate.Operator op, String fqn, Instant v) {
        return predicate(op, fqn, v, FilterPredicate::eq, FilterPredicate::notEq, FilterPredicate::lt,
                FilterPredicate::ltEq, FilterPredicate::gt, FilterPredicate::gtEq);
    }

    private static FilterPredicate uuidPredicate(FilterPredicate.Operator op, String fqn, UUID v) {
        return predicate(op, fqn, v, FilterPredicate::eq, FilterPredicate::notEq, FilterPredicate::lt,
                FilterPredicate::ltEq, FilterPredicate::gt, FilterPredicate::gtEq);
    }

    public static FilterPredicate boolPredicate(FilterPredicate.Operator op, String fqn, boolean v) {
        return switch (op) {
            case EQ -> FilterPredicate.eq(fqn, v);
            case NOT_EQ -> FilterPredicate.notEq(fqn, v);
            default -> null;
        };
    }

    // NOTE: add to PR comment to explain why I went with FunctionInterface
    // public static FilterPredicate intPredicate(String fqn,
    // FilterPredicate.Operator op, int v) {
    // return switch (op) {
    // case EQ -> FilterPredicate.eq(fqn, v);
    // case NOT_EQ -> FilterPredicate.notEq(fqn, v);
    // case LT -> FilterPredicate.lt(fqn, v);
    // case LT_EQ -> FilterPredicate.ltEq(fqn, v);
    // case GT -> FilterPredicate.gt(fqn, v);
    // case GT_EQ -> FilterPredicate.gtEq(fqn, v);
    // };
    // }
    //
    // public static FilterPredicate longPredicate(String fqn,
    // FilterPredicate.Operator op, long v) {
    // return switch (op) {
    // case EQ -> FilterPredicate.eq(fqn, v);
    // case NOT_EQ -> FilterPredicate.notEq(fqn, v);
    // case LT -> FilterPredicate.lt(fqn, v);
    // case LT_EQ -> FilterPredicate.ltEq(fqn, v);
    // case GT -> FilterPredicate.gt(fqn, v);
    // case GT_EQ -> FilterPredicate.gtEq(fqn, v);
    // };
    //
    // ... etc for all the types. Very repetitive.
    //
    // There's also constructors for predicates in Hardwood like IntColumnPredicate,
    // but some types use some private
    // functions to convert the value for Hardwood, like new
    // UUIDColumnPredicate(column, Operator.EQ, uuidToBytes(value));
    // where uuidToBytes is a private method that we can't access. Otherwise we have
    // to copy over that code.
    //
    // I learned about the FunctionInterface approach and this seems to allow us to
    // cut down on repetitive code, as well
    // as still allow the compiler to narrow down the types and find the appropriate
    // overloads in Hardwood
    // }

    public static FilterPredicate createFilterPredicate(Reference ref, Object value, FilterPredicate.Operator op) {
        DataType<?> type = ref.valueType();
        if (type.implicitCast(value) == null) {
            return null;
        }
        String fqn = ref.column().fqn();
        return switch (type.id()) {
            case BooleanType.ID -> boolPredicate(op, fqn, BooleanType.INSTANCE.implicitCast(value));
            case IntegerType.ID -> intPredicate(op, fqn, IntegerType.INSTANCE.implicitCast(value));
            case ShortType.ID -> intPredicate(op, fqn, IntegerType.INSTANCE.implicitCast(value));
            case LongType.ID -> longPredicate(op, fqn, LongType.INSTANCE.implicitCast(value));
            case NumericType.ID -> numericPredicate(op, fqn, NumericType.INSTANCE.implicitCast(value));
            case FloatType.ID -> floatPredicate(op, fqn, FloatType.INSTANCE.implicitCast(value));
            case DoubleType.ID -> doublePredicate(op, fqn, DoubleType.INSTANCE.implicitCast(value));
            case StringType.ID -> stringPredicate(op, fqn, StringType.INSTANCE.implicitCast(value));
            case DateType.ID -> {
                LocalDate localDate = Instant.ofEpochMilli(DateType.INSTANCE.implicitCast(value)).atZone(ZoneOffset.UTC)
                        .toLocalDate();
                yield datePredicate(op, fqn, localDate);
            }
            case TimestampType.ID_WITH_TZ -> {
                Instant instant = Instant.ofEpochMilli(TimestampType.INSTANCE_WITH_TZ.implicitCast(value));
                yield timestampPredicate(op, fqn, instant);
            }
            case UUIDType.ID -> uuidPredicate(op, fqn, UUIDType.INSTANCE.implicitCast(value));
            default -> throw new UnsupportedOperationException("Unexpected error: Cannot push predicate down for " + type.toString());
        };
    }

}
