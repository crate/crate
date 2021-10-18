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

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.lucene.search.Query;

import io.crate.data.Input;
import io.crate.expression.operator.LikeOperators;
import io.crate.expression.operator.Operator;
import io.crate.expression.operator.OperatorModule;
import io.crate.expression.operator.any.AnyRangeOperator.Comparison;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.TypeVariableConstraint;
import io.crate.sql.tree.ComparisonExpression;
import io.crate.types.DataType;
import io.crate.types.TypeSignature;


/**
 * Abstract base class for implementations of {@code <probe> <op> ANY(<candidates>)}
 **/
public abstract sealed class AnyOperator extends Operator<Object>
    permits AnyEqOperator, AnyNeqOperator, AnyRangeOperator, AnyLikeOperator, AnyNotLikeOperator {

    public static final String OPERATOR_PREFIX = "any_";
    public static final List<String> OPERATOR_NAMES = List.of(
        AnyEqOperator.NAME,
        AnyNeqOperator.NAME,
        AnyRangeOperator.Comparison.GT.opName(),
        AnyRangeOperator.Comparison.GTE.opName(),
        AnyRangeOperator.Comparison.LT.opName(),
        AnyRangeOperator.Comparison.LTE.opName(),
        LikeOperators.ANY_LIKE,
        LikeOperators.ANY_ILIKE,
        LikeOperators.ANY_NOT_LIKE,
        LikeOperators.ANY_NOT_ILIKE
    );

    public static final Set<String> SUPPORTED_COMPARISONS = Set.of(
        ComparisonExpression.Type.EQUAL.getValue(),
        ComparisonExpression.Type.NOT_EQUAL.getValue(),
        ComparisonExpression.Type.LESS_THAN.getValue(),
        ComparisonExpression.Type.LESS_THAN_OR_EQUAL.getValue(),
        ComparisonExpression.Type.GREATER_THAN.getValue(),
        ComparisonExpression.Type.GREATER_THAN_OR_EQUAL.getValue()
    );

    private final Signature signature;
    private final Signature boundSignature;
    protected final DataType<Object> leftType;

    public static void register(OperatorModule operatorModule) {
        reg(operatorModule, AnyEqOperator.NAME, (sig, boundSig) -> new AnyEqOperator(sig, boundSig));
        reg(operatorModule, AnyNeqOperator.NAME, (sig, boundSig) -> new AnyNeqOperator(sig, boundSig));
        regRange(operatorModule, AnyRangeOperator.Comparison.GT);
        regRange(operatorModule, AnyRangeOperator.Comparison.GTE);
        regRange(operatorModule, AnyRangeOperator.Comparison.LT);
        regRange(operatorModule, AnyRangeOperator.Comparison.LTE);
    }

    private static void regRange(OperatorModule operatorModule, Comparison comparison) {
        reg(operatorModule, comparison.opName(), (sig, boundSig) -> new AnyRangeOperator(sig, boundSig, comparison));
    }

    private static void reg(OperatorModule module, String name, BiFunction<Signature, Signature, FunctionImplementation> operatorFactory) {
        module.register(
            Signature.scalar(
                name,
                TypeSignature.parseTypeSignature("E"),
                TypeSignature.parseTypeSignature("array(E)"),
                Operator.RETURN_TYPE.getTypeSignature()
            ).withTypeVariableConstraints(TypeVariableConstraint.typeVariable("E")),
            operatorFactory
        );
    }

    @SuppressWarnings("unchecked")
    AnyOperator(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.leftType = (DataType<Object>) boundSignature.getArgumentDataTypes().get(0);
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    abstract boolean matches(Object probe, Object candidate);

    protected abstract Query refMatchesAnyArrayLiteral(Function any, Reference probe, Literal<?> candidates, Context context);

    protected abstract Query literalMatchesAnyArrayRef(Function any, Literal<?> probe, Reference candidates, Context context);

    @Override
    @SafeVarargs
    public final Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object> ... args) {
        assert args != null : "args must not be null";
        assert args.length == 2 : "number of args must be 2";
        assert args[0] != null : "1st argument must not be null";

        Object item = args[0].value();
        Object items = args[1].value();
        if (items == null || item == null) {
            return null;
        }
        boolean anyNulls = false;
        for (Object rightValue : (Iterable<?>) items) {
            if (rightValue == null) {
                anyNulls = true;
                continue;
            }
            if (matches(item, rightValue)) {
                return true;
            }
        }
        return anyNulls ? null : false;
    }

    @Override
    public Query toQuery(Function function, Context context) {
        List<Symbol> args = function.arguments();
        Symbol probe = args.get(0);
        Symbol candidates = args.get(1);
        if (probe instanceof Literal<?> literal && candidates instanceof Reference ref) {
            return literalMatchesAnyArrayRef(function, literal, ref, context);
        } else if (probe instanceof Reference ref && candidates instanceof Literal<?> literal) {
            return refMatchesAnyArrayLiteral(function, ref, literal, context);
        } else {
            return null;
        }
    }
}
