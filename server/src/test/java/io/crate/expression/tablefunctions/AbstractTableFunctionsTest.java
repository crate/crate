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

package io.crate.expression.tablefunctions;

import io.crate.analyze.relations.DocTableRelation;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.table.Operation;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import org.elasticsearch.test.ESTestCase;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataTypes;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.Map;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;

public abstract class AbstractTableFunctionsTest extends ESTestCase {

    protected SqlExpressions sqlExpressions;
    protected TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    @Before
    public void prepareFunctions() {

        DocTableRelation relation = mock(DocTableRelation.class);
        RelationName relationName = new RelationName(null, "t");
        when(relation.getField(any(ColumnIdent.class), any(Operation.class), any(Boolean.class)))
            .thenReturn(new Reference(
                new ReferenceIdent(relationName, "name"),
                RowGranularity.NODE,
                DataTypes.STRING,
                0,
                null));
        sqlExpressions = new SqlExpressions(Map.of(relationName, relation));
    }

    protected Iterable<Row> execute(String expr) {
        Symbol functionSymbol = sqlExpressions.normalize(sqlExpressions.asSymbol(expr));

        var function = (Function) functionSymbol;
        var functionImplementation = (TableFunctionImplementation<?>) sqlExpressions.nodeCtx.functions().getQualified(
            function,
            txnCtx.sessionSettings().searchPath()
        );

        if (functionImplementation.returnType().numElements() > 1) {
            // See classdocs of TableFunctionImplementation for an explanation
            assertThat(
                "If the rowType has multiple elements, the returnType of the boundSignature " +
                "must be an exact match of the returnType",
                functionImplementation.boundSignature().getReturnType().createType(),
                is(functionImplementation.returnType())
            );
        }

        //noinspection unchecked,rawtypes
        return functionImplementation.evaluate(
            txnCtx,
            null,
            function.arguments().stream().map(a -> (Input) a).toArray(Input[]::new));
    }

    public void assertCompile(String functionExpression, java.util.function.Function<Scalar, Matcher<Scalar>> matcher) {
        Symbol functionSymbol = sqlExpressions.asSymbol(functionExpression);
        functionSymbol = sqlExpressions.normalize(functionSymbol);
        assertThat("function expression was normalized, compile would not be hit", functionSymbol, not(instanceOf(Literal.class)));
        Function function = (Function) functionSymbol;
        Scalar scalar = (Scalar) sqlExpressions.nodeCtx.functions().getQualified(function, txnCtx.sessionSettings().searchPath());
        assertThat("Function implementation not found using full qualified lookup", scalar, Matchers.notNullValue());
        Scalar compiled = scalar.compile(function.arguments());
        assertThat(compiled, matcher.apply(scalar));
    }

    protected void assertExecute(String expr, String expected) {
        assertThat(printedTable(execute(expr)), is(expected));
    }
}
