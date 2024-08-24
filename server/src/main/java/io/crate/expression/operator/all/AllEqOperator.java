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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.IntPredicate;
import java.util.stream.StreamSupport;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.jetbrains.annotations.Nullable;

import io.crate.expression.operator.AllOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.Operator;
import io.crate.expression.operator.any.AnyNeqOperator;
import io.crate.expression.predicate.IsNullPredicate;
import io.crate.expression.scalar.ArrayUnnestFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Reference;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.ComparisonExpression;
import io.crate.types.TypeSignature;

public class AllEqOperator extends AllOperator {

    public static String NAME = OPERATOR_PREFIX + ComparisonExpression.Type.EQUAL.getValue();
    public static Signature SIGNATURE = Signature.builder(AllEqOperator.NAME, FunctionType.SCALAR)
            .argumentTypes(TypeSignature.parse("E"), TypeSignature.parse("array(E)"))
            .returnType(Operator.RETURN_TYPE.getTypeSignature())
            .typeVariableConstraints(typeVariable("E"))
            .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
            .build();

    public AllEqOperator(Signature signature, BoundSignature boundSignature, IntPredicate cmp) {
        super(signature, boundSignature, cmp);
    }

    public static Query refMatchesAllArrayLiteral(Reference probe, List<?> values, LuceneQueryBuilder.Context context) {
        // col = ALL ([1,2,3]) --> col=1 and col=2 and col=3
        LinkedHashSet<?> uniqueValues = new LinkedHashSet<>(values);
        if (uniqueValues.contains(null)) {
            return new MatchNoDocsQuery("Cannot match nulls");
        }
        if (uniqueValues.isEmpty()) {
            return new MatchNoDocsQuery("Cannot match unless there is at least one non-null candidate");
        }
        if (uniqueValues.size() > 1) {
            // `col=x and col=y` evaluates to `false` unless `x == y`
            return new MatchNoDocsQuery("A single value cannot match more than one unique values");
        }
        var value = uniqueValues.getFirst();
        // col = all([1]) --> col = 1
        Function eq = new Function(
            EqOperator.SIGNATURE,
            List.of(probe, Literal.ofUnchecked(probe.valueType(), value)),
            EqOperator.RETURN_TYPE
        );
        return context.visitor().visitFunction(eq, context);
    }

    public static Query literalMatchesAllArrayRef(Literal<?> probe, Reference candidates, LuceneQueryBuilder.Context context) {
        // 1 = ALL(array_col)
        // --> 1 = array_col[1] and 1 = array_col[2] and 1 = array_col[3]
        // --> not(1 != array_col[1] or 1 != array_col[2] or 1 != array_col[3])
        // --> not(1 != ANY(array_col))
        return new BooleanQuery.Builder()
            .add(Queries.not(AnyNeqOperator.literalMatchesAnyArrayRef(probe, candidates)), BooleanClause.Occur.MUST)
            .add(IsNullPredicate.refExistsQuery(candidates, context, false), BooleanClause.Occur.FILTER)
            .build();
    }

    @Override
    public @Nullable Query toQuery(Function function, LuceneQueryBuilder.Context context) {
        List<Symbol> args = function.arguments();
        Symbol probe = args.get(0);
        Symbol candidates = args.get(1);
        while (candidates instanceof Function fn && fn.signature().equals(ArrayUnnestFunction.SIGNATURE)) {
            candidates = fn.arguments().get(0);
        }
        if (probe instanceof Literal<?> literal && candidates instanceof Reference ref) {
            return literalMatchesAllArrayRef(literal, ref, context);
        } else if (probe instanceof Reference ref && candidates instanceof Literal<?> literal) {
            var values = StreamSupport
                .stream(((Iterable<?>) literal.value()).spliterator(), false)
                .toList();
            return refMatchesAllArrayLiteral(ref, values, context);
        } else {
            return null;
        }
    }
}
