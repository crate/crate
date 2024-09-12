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
import java.util.stream.StreamSupport;

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.jetbrains.annotations.Nullable;

import io.crate.execution.dml.ArrayIndexer;
import io.crate.expression.operator.AllOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.Operator;
import io.crate.expression.operator.any.AnyNeqOperator;
import io.crate.expression.predicate.NotPredicate;
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
import io.crate.types.ArrayType;
import io.crate.types.TypeSignature;

public class AllEqOperator extends AllOperator {

    public static final String NAME = OPERATOR_PREFIX + ComparisonExpression.Type.EQUAL.getValue();
    public static final Signature SIGNATURE = Signature.builder(AllEqOperator.NAME, FunctionType.SCALAR)
            .argumentTypes(TypeSignature.parse("E"), TypeSignature.parse("array(E)"))
            .returnType(Operator.RETURN_TYPE.getTypeSignature())
            .typeVariableConstraints(typeVariable("E"))
            .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
            .build();

    public AllEqOperator(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature, result -> result == 0);
    }

    public static Query refMatchesAllArrayLiteral(Reference probe, List<?> values, LuceneQueryBuilder.Context context) {
        // col = ALL ([1,2,3]) --> col=1 and col=2 and col=3
        LinkedHashSet<?> uniqueValues = new LinkedHashSet<>(values);
        if (uniqueValues.isEmpty()) {
            return new MatchAllDocsQuery();
        }
        if (uniqueValues.contains(null)) {
            return new MatchNoDocsQuery("Cannot match nulls");
        }
        if (uniqueValues.size() > 1) {
            // `col=x and col=y` evaluates to `false` unless `x == y`
            return new MatchNoDocsQuery("A single value cannot match more than one unique values");
        }
        // col = all([1]) --> col = 1
        var value = uniqueValues.getFirst();
        return EqOperator.fromPrimitive(
            probe.valueType(),
            probe.storageIdent(),
            value,
            probe.hasDocValues(),
            probe.indexType()
        );
    }

    public static Query literalMatchesAllArrayRef(Function allEq, Literal<?> literal, Reference ref, LuceneQueryBuilder.Context context) {
        if (ArrayType.dimensions(ref.valueType()) > 1) {
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
        // 1 = all(array_ref) --> returns true for arrays satisfying the following conditions:
        //   1) arrays without null elements that does not contain values other than the literal
        //   2) empty
        var arraysWithoutNullElementsQuery = ArrayIndexer.arraysWithoutNullElementsQuery(ref, context.tableInfo()::getReference);
        var doesNotContainValuesOtherThanLiteral = Queries.not(AnyNeqOperator.literalMatchesAnyArrayRef(literal, ref)); // not(array_ref > 1 || array_ref < 1)
        var emptyArrays = ArrayIndexer.arrayLengthTermQuery(ref, 0, context.tableInfo()::getReference);
        return new BooleanQuery.Builder()
            .setMinimumNumberShouldMatch(1)
            .add(
                new BooleanQuery.Builder()
                    .add(arraysWithoutNullElementsQuery, Occur.MUST)
                    .add(doesNotContainValuesOtherThanLiteral, Occur.MUST)
                    .build(), Occur.SHOULD)
            .add(emptyArrays, Occur.SHOULD)
            .build();
    }

    @Override
    public @Nullable Query toQuery(Function function, LuceneQueryBuilder.Context context) {
        List<Symbol> args = function.arguments();
        Symbol probe = args.get(0);
        Symbol candidates = args.get(1);
        // CrateDB automatically unnests the array argument to the number of dimensions required
        // i.e. SELECT 1 = ALL([[1, 2], [3, 4]]) -> false
        while (candidates instanceof Function fn && fn.signature().equals(ArrayUnnestFunction.SIGNATURE)) {
            candidates = fn.arguments().get(0);
        }
        if (probe instanceof Literal<?> literal && candidates instanceof Reference ref) {
            return literalMatchesAllArrayRef(function, literal, ref, context);
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
