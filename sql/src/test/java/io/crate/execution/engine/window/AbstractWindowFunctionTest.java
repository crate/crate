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
import io.crate.breaker.RamAccountingContext;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.InputCollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.SearchPath;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.SessionSettings;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.sql.tree.QualifiedName;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataTypes;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.inject.AbstractModule;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.crate.data.SentinelRow.SENTINEL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;


public abstract class AbstractWindowFunctionTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private AbstractModule[] additionalModules;
    private SqlExpressions sqlExpressions;
    private Functions functions;
    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();
    private InputFactory inputFactory;

    private final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker("dummy"));

    public AbstractWindowFunctionTest(AbstractModule... additionalModules) {
        this.additionalModules = additionalModules;
    }

    @Before
    public void prepareFunctions() throws Exception {
        final String tableName = "t1";
        DocTableInfo tableInfo = TestingTableInfo.builder(new RelationName(DocSchemaInfo.NAME, tableName), null)
            .add("x", DataTypes.INTEGER)
            .add("y", DataTypes.LONG)
            .build();
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
                                      int[] orderByIndices,
                                      Object[]... inputRows) {
        performInputSanityChecks(inputRows);

        Symbol normalizedFunctionSymbol = sqlExpressions.normalize(sqlExpressions.asSymbol(functionExpression));
        assertThat(normalizedFunctionSymbol, instanceOf(io.crate.expression.symbol.WindowFunction.class));

        io.crate.expression.symbol.WindowFunction windowFunctionSymbol =
            (io.crate.expression.symbol.WindowFunction) normalizedFunctionSymbol;

        InputFactory.Context<InputCollectExpression> ctx =
            inputFactory.ctxForRefs(txnCtx, r -> new InputCollectExpression(positionInRowByColumn.get(r.column())));
        List<Symbol> allInputSymbols = extractSymbolsOnceMaintainOrder(windowFunctionSymbol);
        ctx.add(allInputSymbols);

        InputFactory.Context<CollectExpression<Row, ?>> ctxForPartitions = inputFactory.ctxForRefs(txnCtx, r -> new InputCollectExpression(positionInRowByColumn.get(r.column())));
        ctxForPartitions.add(windowFunctionSymbol.windowDefinition().partitions());

        FunctionImplementation impl = functions.getQualified(windowFunctionSymbol.info().ident());
        assert(impl instanceof WindowFunction): "General-purpose window functions are supported only";
        WindowFunction windowFunctionImpl = (WindowFunction) impl;

        WindowBatchIterator iterator = new WindowBatchIterator(
            windowFunctionSymbol.windowDefinition(),
            Collections.emptyList(),
            Collections.emptyList(),
            InMemoryBatchIterator.of(Arrays.stream(inputRows).map(RowN::new).collect(Collectors.toList()), SENTINEL),
            Collections.singletonList(windowFunctionImpl),
            ctx.expressions(),
            ctxForPartitions.expressions(),
            ctxForPartitions.topLevelInputs(),
            // remove all Literal instances from standaloneInputTypes
            // as these will not be included in inputRows
            allInputSymbols.stream()
                .filter(s -> ! (s instanceof Literal))
                .map(Symbol::valueType)
                .collect(Collectors.toList()),
            RAM_ACCOUNTING_CONTEXT,
            orderByIndices,
            ctx.topLevelInputs().toArray(new Input[0])
        );

        List<Object> actualResult = new ArrayList<>();
        while (iterator.moveNext()) {
            actualResult.add(iterator.currentElement().get(0));
        }

        assertThat((T) actualResult, expectedValue);
    }

    private List<Symbol> extractSymbolsOnceMaintainOrder(io.crate.expression.symbol.WindowFunction windowFunctionSymbol) {
        List<Symbol> allSymbols = new ArrayList<>();
        allSymbols.addAll(windowFunctionSymbol.arguments());
        allSymbols.addAll(windowFunctionSymbol.windowDefinition().partitions());
        OrderBy orderBy = windowFunctionSymbol.windowDefinition().orderBy();
        if (orderBy != null) {
            allSymbols.addAll(orderBy.orderBySymbols());
        }
        return allSymbols.stream().distinct().collect(Collectors.toList());
    }
}
