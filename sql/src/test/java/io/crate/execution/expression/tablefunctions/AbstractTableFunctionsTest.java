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

package io.crate.execution.expression.tablefunctions;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Bucket;
import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.stream.Collectors;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.mockito.Mockito.mock;

public abstract class AbstractTableFunctionsTest extends CrateUnitTest {

    private SqlExpressions sqlExpressions;
    private Functions functions;

    @Before
    public void prepareFunctions() throws Exception {
        sqlExpressions = new SqlExpressions(ImmutableMap.of(QualifiedName.of("t"), mock(DocTableRelation.class)));
        functions = sqlExpressions.getInstance(Functions.class);
    }

    protected Bucket execute(String expr) {
        Symbol functionSymbol = sqlExpressions.asSymbol(expr);
        functionSymbol = sqlExpressions.normalize(functionSymbol);
        Function function = (Function) functionSymbol;
        FunctionIdent ident = function.info().ident();
        TableFunctionImplementation tableFunction = (TableFunctionImplementation)
            functions.getBuiltin(ident.name(), ident.argumentTypes());
        return tableFunction.execute(function.arguments().stream().map(a -> (Input) a).collect(Collectors.toList()));
    }

    protected void assertExecute(String expr, String expected) {
        assertThat(printedTable(execute(expr)), Matchers.is(expected));
    }
}
