/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.tablefunctions;

import io.crate.analyze.relations.DocTableRelation;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import org.junit.Before;

import java.util.Map;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public abstract class AbstractTableFunctionsTest extends CrateUnitTest {

    protected SqlExpressions sqlExpressions;
    protected Functions functions;
    protected TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    @Before
    public void prepareFunctions() {
        sqlExpressions = new SqlExpressions(Map.of(new RelationName(null, "t"), mock(DocTableRelation.class)));
        functions = sqlExpressions.getInstance(Functions.class);
    }

    protected Iterable<Row> execute(String expr) {
        Symbol functionSymbol = sqlExpressions.normalize(sqlExpressions.asSymbol(expr));

        var function = (Function) functionSymbol;
        var functionImplementation = (TableFunctionImplementation<?>) functions.getQualified(
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
            function.arguments().stream().map(a -> (Input) a).toArray(Input[]::new));
    }

    protected void assertExecute(String expr, String expected) {
        assertThat(printedTable(execute(expr)), is(expected));
    }
}
