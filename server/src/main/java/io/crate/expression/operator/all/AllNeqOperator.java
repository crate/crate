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

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;

import io.crate.expression.operator.Operator;
import io.crate.expression.operator.any.AnyEqOperator;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Reference;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.ComparisonExpression;
import io.crate.types.TypeSignature;

public final class AllNeqOperator extends AllOperator<Object> {

    public static final String NAME = OPERATOR_PREFIX + ComparisonExpression.Type.NOT_EQUAL.getValue();
    public static final Signature SIGNATURE = Signature.builder(NAME, FunctionType.SCALAR)
        .argumentTypes(TypeSignature.parse("E"), TypeSignature.parse("array(E)"))
        .returnType(Operator.RETURN_TYPE.getTypeSignature())
        .typeVariableConstraints(typeVariable("E"))
        .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
        .build();

    public AllNeqOperator(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    boolean matches(Object probe, Object candidate) {
        return leftType.compare(probe, candidate) != 0;
    }

    @Override
    protected Query refMatchesAllArrayLiteral(Function allNeq, Reference probe, Literal<?> literal, LuceneQueryBuilder.Context context) {
        var uniqueValues = StreamSupport
            .stream(((Iterable<?>) literal.value()).spliterator(), false)
            .collect(Collectors.toSet());
        if (uniqueValues.isEmpty()) {
            return new MatchAllDocsQuery();
        }
        if (uniqueValues.contains(null)) {
            // i.e.::
            //postgres=# select 1 != all(array[1, null]);
            // ?column?
            //----------
            // f
            //(1 row)
            //
            //postgres=# select 1 != all(array[2, null]);
            // ?column?
            //----------
            //
            //(1 row)
            return new MatchNoDocsQuery("If the array literal contains nulls, it is either false or null; hence a no-match");
        }
        return toNotAnyEq(allNeq, context);
    }

    @Override
    protected Query literalMatchesAllArrayRef(Function allNeq, Literal<?> probe, Reference candidates, LuceneQueryBuilder.Context context) {
        return toNotAnyEq(allNeq, context);
    }

    private Query toNotAnyEq(Function allNeq, LuceneQueryBuilder.Context context) {
        // col != ALL ([1,2,3]) --> col != 1 AND col != 2 AND col != 3 --> ! col = ANY([1,2,3])
        Function anyEq = new Function(
            AnyEqOperator.SIGNATURE,
            allNeq.arguments(),
            Operator.RETURN_TYPE
        );
        Function notAnyEq = new Function(
            NotPredicate.SIGNATURE,
            List.of(anyEq),
            Operator.RETURN_TYPE
        );
        return context.visitor().visitFunction(notAnyEq, context);
    }
}
