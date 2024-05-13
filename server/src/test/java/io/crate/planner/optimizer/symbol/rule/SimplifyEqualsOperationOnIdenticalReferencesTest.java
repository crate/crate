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

package io.crate.planner.optimizer.symbol.rule;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.exceptions.ConversionException;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.predicate.IsNullPredicate;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.scalar.Ignore3vlFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.planner.optimizer.symbol.FunctionSymbolResolver;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;


public class SimplifyEqualsOperationOnIdenticalReferencesTest {
    private static final NodeContext NODE_CONTEXT = TestingHelpers.createNodeContext();
    private static final FunctionSymbolResolver FUNCTION_SYMBOL_RESOLVER = (f, args) -> {
        try {
            return ExpressionAnalyzer.allocateFunction(
                f,
                args,
                null,
                null,
                CoordinatorTxnCtx.systemTransactionContext(),
                NODE_CONTEXT);
        } catch (ConversionException e) {
            return null;
        }
    };
    private static final SimplifyEqualsOperationOnIdenticalReferences RULE =
        new SimplifyEqualsOperationOnIdenticalReferences(FUNCTION_SYMBOL_RESOLVER);
    private static final Symbol NULLABLE_REF = new SimpleReference(
        new ReferenceIdent(new RelationName(null, "dummy"), "col"),
        RowGranularity.DOC,
        DataTypes.INTEGER,
        ColumnPolicy.DYNAMIC,
        IndexType.PLAIN,
        true,
        false,
        0,
        111,
        true,
        null
    );
    private static final Symbol NOT_NULLABLE_REF = new SimpleReference(
        new ReferenceIdent(new RelationName(null, "dummy"), "col"),
        RowGranularity.DOC,
        DataTypes.INTEGER,
        ColumnPolicy.DYNAMIC,
        IndexType.PLAIN,
        false,
        false,
        0,
        112,
        true,
        null
    );

    @Test
    public void testSimplifyRefEqRefWhenRefIsNullableWithNoParent() {
        Function functionToOptimize = new Function(
            EqOperator.SIGNATURE,
            List.of(
                NULLABLE_REF,
                NULLABLE_REF
            ),
            DataTypes.BOOLEAN);
        Match<Function> match = RULE.pattern().accept(functionToOptimize, Captures.empty());
        assertTrue(match.isPresent());
        Symbol optimizedFunction = RULE.apply(match.value(), match.captures(), NODE_CONTEXT, null);
        Symbol expected = new Function(
            NotPredicate.SIGNATURE,
            List.of(
                new Function(
                    IsNullPredicate.SIGNATURE,
                    List.of(
                        NULLABLE_REF
                    ),
                    DataTypes.BOOLEAN
                )
            ),
            DataTypes.BOOLEAN
        );
        assertThat(optimizedFunction, is(expected));
    }

    @Test
    public void testSimplifyRefEqRefWhenRefIsNullableWithParent() {
        Function functionToOptimize = new Function(
            EqOperator.SIGNATURE,
            List.of(
                NULLABLE_REF,
                NULLABLE_REF
            ),
            DataTypes.BOOLEAN);
        Function dummyParent = new Function(
            NotPredicate.SIGNATURE,
            List.of(functionToOptimize),
            DataTypes.BOOLEAN
        );
        Match<Function> match = RULE.pattern().accept(functionToOptimize, Captures.empty());
        assertTrue(match.isPresent());
        Symbol optimizedFunction = RULE.apply(match.value(), match.captures(), NODE_CONTEXT, dummyParent);
        assertThat(optimizedFunction, is(nullValue()));
    }

    @Test
    public void testSimplifyRefEqRefWhenRefIsNotNullable() {
        Function functionToOptimize = new Function(
            EqOperator.SIGNATURE,
            List.of(
                NOT_NULLABLE_REF,
                NOT_NULLABLE_REF
            ),
            DataTypes.BOOLEAN);
        Match<Function> match = RULE.pattern().accept(functionToOptimize, Captures.empty());
        assertTrue(match.isPresent());
        // function to optimize has no parent
        Symbol optimizedFunction = RULE.apply(match.value(), match.captures(), NODE_CONTEXT, null);
        assertThat(optimizedFunction, is(Literal.BOOLEAN_TRUE));

        // function to optimize has a parent
        Function dummyParent = new Function(
            NotPredicate.SIGNATURE,
            List.of(functionToOptimize),
            DataTypes.BOOLEAN
        );
        optimizedFunction = RULE.apply(match.value(), match.captures(), NODE_CONTEXT, dummyParent);
        assertThat(optimizedFunction, is(Literal.BOOLEAN_TRUE));
    }

    @Test
    public void testSimplifyRefEqRefWhenParentIsIgnore3vlFunction() {
        Function func = new Function(
            Ignore3vlFunction.SIGNATURE,
            List.of(
                new Function(
                    EqOperator.SIGNATURE,
                    List.of(
                        NULLABLE_REF,
                        NULLABLE_REF
                    ),
                    DataTypes.BOOLEAN)
            ),
            DataTypes.BOOLEAN);
        Match<Function> match = RULE.pattern().accept(func.arguments().get(0), Captures.empty());
        assertTrue(match.isPresent());
        Symbol optimizedFunction = RULE.apply(match.value(), match.captures(), NODE_CONTEXT, func);
        assertThat(optimizedFunction, is(Literal.BOOLEAN_TRUE));
    }
}
