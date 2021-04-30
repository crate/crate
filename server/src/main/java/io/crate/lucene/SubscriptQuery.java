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

import io.crate.data.Input;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.GtOperator;
import io.crate.expression.operator.GteOperator;
import io.crate.expression.operator.LtOperator;
import io.crate.expression.operator.LteOperator;
import io.crate.expression.scalar.SubscriptFunction;
import io.crate.expression.symbol.Function;
import io.crate.metadata.Reference;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

class SubscriptQuery implements InnerFunctionToQuery {

    private interface PreFilterQueryBuilder {
        Query buildQuery(MappedFieldType fieldType, Object value, QueryShardContext context);
    }

    private static final Map<String, PreFilterQueryBuilder> PRE_FILTER_QUERY_BUILDER_BY_OP = Map.of(
        EqOperator.NAME, MappedFieldType::termQuery,
        GteOperator.NAME, (fieldType, value, context)
            -> fieldType.rangeQuery(value, null, true, false, null, null, context),
        GtOperator.NAME, (fieldType, value, context)
            -> fieldType.rangeQuery(value, null, false, false, null, null, context),
        LteOperator.NAME, (fieldType, value, context)
            -> fieldType.rangeQuery(null, value, false, true, null, null, context),
        LtOperator.NAME, (fieldType, value, context)
            -> fieldType.rangeQuery(null, value, false, false, null, null, context)
    );

    @Nullable
    @Override
    public Query apply(Function parent, Function inner, LuceneQueryBuilder.Context context) throws IOException {
        assert inner.name().equals(SubscriptFunction.NAME) :
            "function must be " + SubscriptFunction.NAME;

        RefAndLiteral innerPair = RefAndLiteral.of(inner);
        if (innerPair == null) {
            return null;
        }
        Reference reference = innerPair.reference();

        if (DataTypes.isArray(innerPair.reference().valueType())) {
            PreFilterQueryBuilder preFilterQueryBuilder =
                PRE_FILTER_QUERY_BUILDER_BY_OP.get(parent.name());
            if (preFilterQueryBuilder == null) {
                return null;
            }

            FunctionLiteralPair functionLiteralPair = new FunctionLiteralPair(parent);
            if (!functionLiteralPair.isValid()) {
                return null;
            }
            Input<?> input = functionLiteralPair.input();

            MappedFieldType fieldType = context.getFieldTypeOrNull(reference.column().fqn());
            if (fieldType == null) {
                if (ArrayType.unnest(reference.valueType()).id() == ObjectType.ID) {
                    return null; // fallback to generic query to enable objects[1] = {x=10}
                }
                return Queries.newMatchNoDocsQuery("column doesn't exist in this index");
            }
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(
                preFilterQueryBuilder.buildQuery(fieldType, input.value(), context.queryShardContext),
                BooleanClause.Occur.MUST);
            builder.add(LuceneQueryBuilder.genericFunctionFilter(parent, context), BooleanClause.Occur.FILTER);
            return builder.build();
        }
        return null;
    }
}
