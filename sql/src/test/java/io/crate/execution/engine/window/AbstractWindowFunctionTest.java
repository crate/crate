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

package io.crate.execution.engine.window;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.auth.user.User;
import io.crate.data.BatchIterator;
import io.crate.data.BatchIterators;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.execution.engine.collect.InputCollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import org.elasticsearch.common.inject.AbstractModule;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.crate.data.SentinelRow.SENTINEL;
import static io.crate.execution.engine.sort.Comparators.createComparator;
import static org.hamcrest.Matchers.instanceOf;


public abstract class AbstractWindowFunctionTest extends CrateDummyClusterServiceUnitTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private AbstractModule[] additionalModules;
    private SqlExpressions sqlExpressions;
    private Functions functions;
    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();
    private InputFactory inputFactory;

    public AbstractWindowFunctionTest(AbstractModule... additionalModules) {
        this.additionalModules = additionalModules;
    }

    @Before
    public void prepareFunctions() throws Exception {
        final String tableName = "t1";
        DocTableInfo tableInfo = SQLExecutor.tableInfo(
            new RelationName("doc", "t1"),
            "create table doc.t1 (x int, y bigint)",
            clusterService);
        DocTableRelation tableRelation = new DocTableRelation(tableInfo);
        Map<QualifiedName, AnalyzedRelation> tableSources = ImmutableMap.of(new QualifiedName(tableName), tableRelation);
        sqlExpressions = new SqlExpressions(tableSources, tableRelation,
            null, User.CRATE_USER, additionalModules);
        functions = sqlExpressions.functions();
        inputFactory = new InputFactory(functions);
    }


    private void performInputSanityChecks(Object[]... inputs) {
        List<Integer> inputSizes = Arrays.stream(inputs)
            .map(Array::getLength)
            .distinct()
            .collect(Collectors.toList()
            );

        if (inputSizes.size() != 1) {
            throw new IllegalArgumentException("Inputs need to be of equal size");
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> void assertEvaluate(String functionExpression,
                                      Matcher<T> expectedValue,
                                      Map<ColumnIdent, Integer> positionInRowByColumn,
                                      Object[]... inputRows) throws Exception {
        performInputSanityChecks(inputRows);

        Symbol normalizedFunctionSymbol = sqlExpressions.normalize(sqlExpressions.asSymbol(functionExpression));
        assertThat(normalizedFunctionSymbol, instanceOf(io.crate.expression.symbol.WindowFunction.class));

        io.crate.expression.symbol.WindowFunction windowFunctionSymbol =
            (io.crate.expression.symbol.WindowFunction) normalizedFunctionSymbol;
        ReferenceResolver<InputCollectExpression> referenceResolver =
            r -> new InputCollectExpression(positionInRowByColumn.get(r.column()));

        var argsCtx = inputFactory.ctxForRefs(txnCtx, referenceResolver);
        argsCtx.add(windowFunctionSymbol.arguments());

        FunctionImplementation impl = functions.getQualified(windowFunctionSymbol.info().ident());
        assert impl instanceof WindowFunction: "Got " + impl + " but expected a window function";
        WindowFunction windowFunctionImpl = (WindowFunction) impl;

        int numCellsInSourceRows = inputRows[0].length;
        var windowDef = windowFunctionSymbol.windowDefinition();
        var partitionOrderBy = windowDef.partitions().isEmpty() ? null : new OrderBy(windowDef.partitions());
        BatchIterator<Row> iterator = WindowFunctionBatchIterator.of(
            InMemoryBatchIterator.of(Arrays.stream(inputRows).map(RowN::new).collect(Collectors.toList()), SENTINEL),
            new IgnoreRowAccounting(),
            createComparator(() -> inputFactory.ctxForRefs(txnCtx, referenceResolver), partitionOrderBy),
            createComparator(() -> inputFactory.ctxForRefs(txnCtx, referenceResolver), windowDef.orderBy()),
            numCellsInSourceRows,
            () -> 1,
            Runnable::run,
            List.of(windowFunctionImpl),
            argsCtx.expressions(),
            new Input[][]{argsCtx.topLevelInputs().toArray(new Input[0])}
        );
        List<Object> actualResult = BatchIterators.collect(
            iterator,
            Collectors.mapping(row -> row.get(numCellsInSourceRows), Collectors.toList())).get(5, TimeUnit.SECONDS);
        assertThat((T) actualResult, expectedValue);
    }
}
