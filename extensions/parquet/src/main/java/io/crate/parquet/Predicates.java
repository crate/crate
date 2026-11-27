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


import dev.hardwood.reader.FilterPredicate;
import io.crate.metadata.Reference;
import io.crate.types.DataType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;

public class Predicates {

    @FunctionalInterface
    interface FilterPredicateFactory<T> {
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

    public static FilterPredicate createFilterPredicate(Reference ref, Object value, FilterPredicate.Operator op) {
        DataType<?> type = ref.valueType();
        if (type.implicitCast(value) == null) {
            return null;
        }
        String fqn = ref.column().fqn();
        return switch (type.id()) {
            case IntegerType.ID -> predicate(op, fqn, IntegerType.INSTANCE.implicitCast(value), FilterPredicate::eq, FilterPredicate::notEq, FilterPredicate::lt,
                FilterPredicate::ltEq, FilterPredicate::gt, FilterPredicate::gtEq);
            case LongType.ID -> predicate(op, fqn, LongType.INSTANCE.implicitCast(value), FilterPredicate::eq, FilterPredicate::notEq, FilterPredicate::lt,
                FilterPredicate::ltEq, FilterPredicate::gt, FilterPredicate::gtEq);
            default -> throw new UnsupportedOperationException("Unexpected error: Cannot push predicate down for " + type.toString());
        };
    }

}
