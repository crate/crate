/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.operator.all;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;

import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.Operator;
import io.crate.expression.operator.any.AnyNeqOperator;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.Reference;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.ComparisonExpression;

public final class AllEqOperator extends AllOperator<Object> {

    public static final String NAME = OPERATOR_PREFIX + ComparisonExpression.Type.EQUAL.getValue();

    public AllEqOperator(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    boolean matches(Object probe, Object candidate) {
        return leftType.compare(probe, candidate) == 0;
    }

    @Override
    protected Query refMatchesAllArrayLiteral(Function all, Reference probe, Literal<?> literal, LuceneQueryBuilder.Context context) {
        // col = ALL ([1,2,3]) --> col=1 and col=2 and col=3
        var uniqueValues = StreamSupport
            .stream(((Iterable<?>) literal.value()).spliterator(), false)
            .collect(Collectors.toSet());
        if (uniqueValues.isEmpty()) {
            return new MatchAllDocsQuery();
        }
        if (uniqueValues.contains(null)) {
            // i.e.::
            // postgres=# select 1 = all(array[null, 1]);
            // ?column?
            //----------
            //
            //(1 row)
            //
            //postgres=# select 1 = all(array[null, 2]);
            // ?column?
            //----------
            // f
            //(1 row)
            return new MatchNoDocsQuery("If the array literal contains nulls, it is either false or null; hence a no-match");
        }
        if (uniqueValues.size() > 1) {
            // `col=x and col=y` evaluates to `false` unless `x == y`
            return new MatchNoDocsQuery("A single value cannot match more than one unique values");
        }
        // col = all([1]) --> col = 1
        var value = uniqueValues.iterator().next();
        return EqOperator.fromPrimitive(
            probe.valueType(),
            probe.storageIdent(),
            value,
            probe.hasDocValues(),
            probe.indexType()
        );
    }

    @Override
    protected Query literalMatchesAllArrayRef(Function allEq, Literal<?> literal, Reference ref, LuceneQueryBuilder.Context context) {
        // 1 = ALL(array_col)
        // --> 1 = array_col[1] and 1 = array_col[2] and 1 = array_col[3]
        // --> not(1 != array_col[1] or 1 != array_col[2] or 1 != array_col[3])
        // --> not(1 != ANY(array_col))
        Function notAnyNeq = new Function(
            NotPredicate.SIGNATURE,
            List.of(
                new Function(
                    AnyNeqOperator.SIGNATURE,
                    allEq.arguments(),
                    Operator.RETURN_TYPE
                )
            ),
            Operator.RETURN_TYPE
        );
        return context.visitor().visitFunction(notAnyNeq, context);
    }
}
