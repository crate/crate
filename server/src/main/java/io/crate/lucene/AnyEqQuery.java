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
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static io.crate.lucene.AbstractAnyQuery.toIterable;
import static io.crate.lucene.LuceneQueryBuilder.genericFunctionFilter;

class AnyEqQuery implements FunctionToQuery {

    @Nullable
    @Override
    public Query apply(Function input, LuceneQueryBuilder.Context context) {
        List<Symbol> args = input.arguments();

        Symbol candidate = args.get(0);
        Symbol array = args.get(1);

        if (candidate instanceof Literal && array instanceof Reference) {
            return literalMatchesAnyArrayRef(input, (Literal) candidate, (Reference) array, context);
        } else if (candidate instanceof Reference && array instanceof Literal) {
            return refMatchesAnyArrayLiteral((Reference) candidate, (Literal) array, context);
        } else {
            return genericFunctionFilter(input, context);
        }
    }

    private static Query literalMatchesAnyArrayRef(Function any,
                                                   Literal candidate,
                                                   Reference array,
                                                   LuceneQueryBuilder.Context context) {
        MappedFieldType fieldType = context.getFieldTypeOrNull(array.column().fqn());
        if (fieldType == null) {
            if (ArrayType.unnest(array.valueType()).id() == ObjectType.ID) {
                return genericFunctionFilter(any, context); // {x=10} = any(objects)
            }
            return Queries.newMatchNoDocsQuery("column doesn't exist in this index");
        }
        if (DataTypes.isArray(candidate.valueType())) {
            return arrayLiteralEqAnyArray(any, fieldType, candidate.value(), context);
        }
        return fieldType.termQuery(candidate.value(), context.queryShardContext());
    }

    private static Query refMatchesAnyArrayLiteral(Reference candidate,
                                                   Literal array,
                                                   LuceneQueryBuilder.Context context) {
        String columnName = candidate.column().fqn();
        return LuceneQueryBuilder.termsQuery(context.getFieldTypeOrNull(columnName), LuceneQueryBuilder.asList(array), context.queryShardContext);
    }

    private static Query arrayLiteralEqAnyArray(Function function,
                                                MappedFieldType fieldType,
                                                Object candidate,
                                                LuceneQueryBuilder.Context context) {
        ArrayList<Object> terms = new ArrayList<>();
        gatherLeafs(toIterable(candidate), terms::add);

        return new BooleanQuery.Builder()
            .add(fieldType.termsQuery(terms, context.queryShardContext), BooleanClause.Occur.MUST)
            .add(genericFunctionFilter(function, context), BooleanClause.Occur.FILTER)
            .build();
    }

    private static void gatherLeafs(Iterable<?> toIterable, Consumer<? super Object> consumeLeaf) {
        for (Object o : toIterable) {
            if (o instanceof Iterable) {
                gatherLeafs(((Iterable) o), consumeLeaf);
            } else if (o instanceof Object[]) {
                gatherLeafs(Arrays.asList(((Object[]) o)), consumeLeaf);
            } else {
                consumeLeaf.accept(o);
            }
        }
    }
}
