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

package io.crate.expression.operator;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import java.util.List;

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.jetbrains.annotations.Nullable;

import io.crate.execution.dml.ArrayIndexer;
import io.crate.expression.operator.any.AnyEqOperator;
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
import io.crate.types.TypeSignature;

public class AllNeqOperator extends AllOperator {

    public static final String NAME = OPERATOR_PREFIX + ComparisonExpression.Type.NOT_EQUAL.getValue();
    public static final Signature SIGNATURE = Signature.builder(AllNeqOperator.NAME, FunctionType.SCALAR)
        .argumentTypes(TypeSignature.parse("E"), TypeSignature.parse("array(E)"))
        .returnType(Operator.RETURN_TYPE.getTypeSignature())
        .typeVariableConstraints(typeVariable("E"))
        .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
        .build();

    public AllNeqOperator(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature, result -> result != 0);
    }

    public static Query refMatchesAllArrayLiteral(Function allNeq, LuceneQueryBuilder.Context context) {
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

    public static Query literalMatchesAllArrayRef(Literal<?> literal, Reference reference, LuceneQueryBuilder.Context context) {
        // 1 != all(array_ref) --> returns true for arrays satisfying the following conditions:
        //   1) at least one non-null element that is not equal to 1
        //   2) empty
        var arraysWithAtLeastOneNonNull = ArrayIndexer.arraysWithAtLeastOneNonNullElementQuery(reference);
        var arraysWithAtLeastOneNonNullAndNeq = new BooleanQuery.Builder()
            .add(arraysWithAtLeastOneNonNull, Occur.MUST)
            .add(
                EqOperator.fromPrimitive(
                    reference.valueType(),
                    reference.storageIdent(),
                    literal.value(),
                    reference.hasDocValues(),
                    reference.indexType()),
                Occur.MUST_NOT)
            .build();
        var emptyArrays = ArrayIndexer.arrayLengthTermQuery(reference, 0, context.tableInfo()::getReference);
        return new BooleanQuery.Builder()
            .setMinimumNumberShouldMatch(1)
            .add(arraysWithAtLeastOneNonNullAndNeq, Occur.SHOULD)
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
        if (probe instanceof Literal<?> literal && candidates instanceof Reference reference) {
            return literalMatchesAllArrayRef(literal, reference, context);
        } else if (probe instanceof Reference && candidates instanceof Literal<?>) {
            return refMatchesAllArrayLiteral(function, context);
        } else {
            return null;
        }
    }
}
