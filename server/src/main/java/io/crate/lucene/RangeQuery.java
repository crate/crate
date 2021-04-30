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

package io.crate.lucene;

import io.crate.expression.symbol.Function;
import io.crate.metadata.Reference;
import org.apache.lucene.search.Query;
import io.crate.common.collections.Tuple;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;

class RangeQuery implements FunctionToQuery {

    private final boolean includeLower;
    private final boolean includeUpper;
    private final java.util.function.Function<Object, Tuple<?, ?>> boundsFunction;

    private static final java.util.function.Function<Object, Tuple<?, ?>> LOWER_BOUND = in -> new Tuple<>(in, null);
    private static final java.util.function.Function<Object, Tuple<?, ?>> UPPER_BOUND = in -> new Tuple<>(null, in);

    RangeQuery(String comparison) {
        switch (comparison) {
            case "lt":
                boundsFunction = UPPER_BOUND;
                includeLower = false;
                includeUpper = false;
                break;
            case "gt":
                boundsFunction = LOWER_BOUND;
                includeLower = false;
                includeUpper = false;
                break;
            case "lte":
                boundsFunction = UPPER_BOUND;
                includeLower = false;
                includeUpper = true;
                break;
            case "gte":
                boundsFunction = LOWER_BOUND;
                includeLower = true;
                includeUpper = false;
                break;
            default:
                throw new IllegalArgumentException("invalid comparison");
        }
    }

    @Override
    public Query apply(Function input, LuceneQueryBuilder.Context context) {
        RefAndLiteral refAndLiteral = RefAndLiteral.of(input);
        if (refAndLiteral == null) {
            return null;
        }
        return toQuery(
            refAndLiteral.reference(),
            refAndLiteral.literal().value(),
            context::getFieldTypeOrNull,
            context.queryShardContext);
    }

    Query toQuery(Reference reference, Object value, FieldTypeLookup fieldTypeLookup, QueryShardContext queryShardContext) {
        String columnName = reference.column().fqn();
        MappedFieldType fieldType = fieldTypeLookup.get(columnName);
        if (fieldType == null) {
            // can't match column that doesn't exist or is an object ( "o >= {x=10}" is not supported)
            return Queries.newMatchNoDocsQuery("column does not exist in this index");
        }
        Tuple<?, ?> bounds = boundsFunction.apply(value);
        assert bounds != null : "bounds must not be null";
        return fieldType.rangeQuery(bounds.v1(), bounds.v2(), includeLower, includeUpper, null, null, queryShardContext);
    }
}
