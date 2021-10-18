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

package io.crate.expression.operator.any;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;

import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.Reference;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.ComparisonExpression;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

public final class AnyEqOperator extends AnyOperator {

    public static String NAME = OPERATOR_PREFIX + ComparisonExpression.Type.EQUAL.getValue();

    AnyEqOperator(Signature signature, Signature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    boolean matches(Object probe, Object candidate) {
        return leftType.compare(probe, candidate) == 0;
    }

    @Override
    protected Query refMatchesAnyArrayLiteral(Function any, Reference probe, Literal<?> candidates, Context context) {
        String columnName = probe.column().fqn();
        List<?> values = (List<?>) candidates.value();
        MappedFieldType fieldType = context.getFieldTypeOrNull(columnName);
        if (fieldType == null) {
            return Queries.newMatchNoDocsQuery("column does not exist in this index");
        }
        DataType<?> innerType = ArrayType.unnest(probe.valueType());
        return EqOperator.termsQuery(columnName, (DataType) innerType, (List) values);
    }

    @Override
    protected Query literalMatchesAnyArrayRef(Function any, Literal<?> probe, Reference candidates, Context context) {
        MappedFieldType fieldType = context.getFieldTypeOrNull(candidates.column().fqn());
        if (fieldType == null) {
            if (ArrayType.unnest(candidates.valueType()).id() == ObjectType.ID) {
                // {x=10} = any(objects)
                return null;
            }
            return Queries.newMatchNoDocsQuery("column doesn't exist in this index");
        }
        if (DataTypes.isArray(probe.valueType())) {
            // [1, 2] = any(nested_array_ref)
            return arrayLiteralEqAnyArray(any, candidates, probe.value(), context);
        }
        return EqOperator.fromPrimitive(ArrayType.unnest(candidates.valueType()), candidates.column().fqn(), probe.value());
    }

    private static Query arrayLiteralEqAnyArray(Function function,
                                                Reference candidates,
                                                Object candidate,
                                                LuceneQueryBuilder.Context context) {
        ArrayList<Object> terms = new ArrayList<>();
        gatherLeafs((Iterable<?>) candidate, terms::add);
        Query termsQuery = EqOperator.termsQuery(
            candidates.column().fqn(),
            ArrayType.unnest(candidates.valueType()),
            terms
        );
        Query genericFunctionFilter = LuceneQueryBuilder.genericFunctionFilter(function, context);
        if (termsQuery == null) {
            return genericFunctionFilter;
        }
        return new BooleanQuery.Builder()
            .add(termsQuery, Occur.MUST)
            .add(genericFunctionFilter, Occur.FILTER)
            .build();
    }

    private static void gatherLeafs(Iterable<?> toIterable, Consumer<? super Object> consumeLeaf) {
        for (Object o : toIterable) {
            if (o instanceof Iterable<?> nestedIterable) {
                gatherLeafs(nestedIterable, consumeLeaf);
            } else if (o instanceof Object[]) {
                gatherLeafs(Arrays.asList(((Object[]) o)), consumeLeaf);
            } else {
                consumeLeaf.accept(o);
            }
        }
    }
}
