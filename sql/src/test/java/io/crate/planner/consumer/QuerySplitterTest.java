/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner.consumer;

import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.FieldProvider;
import io.crate.analyze.relations.TableRelation;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.symbol.Field;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.QualifiedName;
import io.crate.testing.MockedClusterServiceModule;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;

import static io.crate.testing.TestingHelpers.assertLiteralSymbol;
import static io.crate.testing.TestingHelpers.isFunction;
import static io.crate.testing.TestingHelpers.newMockedThreadPool;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QuerySplitterTest {

    private ExpressionAnalyzer expressionAnalyzer;
    private ExpressionAnalysisContext expressionAnalysisCtx;
    private TableRelation t1;
    private TableRelation t2;

    @Before
    public void setUp() throws Exception {
        Injector injector = new ModulesBuilder()
                .add(new MockedClusterServiceModule())
                .add(new OperatorModule())
                .add(new MetaDataModule())
                .add(new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(ThreadPool.class).toInstance(newMockedThreadPool());
                    }
                })
                .add(new ScalarFunctionModule()).createInjector();

        TableInfo tableInfo = mock(TableInfo.class);
        SchemaInfo schemaInfo = mock(SchemaInfo.class);
        when(schemaInfo.name()).thenReturn("doc");
        when(tableInfo.schemaInfo()).thenReturn(schemaInfo);
        t1 = new TableRelation(tableInfo);
        t2 = new TableRelation(tableInfo);
        expressionAnalysisCtx = new ExpressionAnalysisContext();
        expressionAnalyzer = new ExpressionAnalyzer(
                injector.getInstance(AnalysisMetaData.class),
                new ParameterContext(new Object[0], new Object[0][], null),
                new FieldProvider() {
                    @Override
                    public Field resolveField(QualifiedName qualifiedName, boolean forWrite) {
                        List<String> parts = qualifiedName.getParts();
                        if (parts.get(0).equals("t1")) {
                            return new Field(t1, new ColumnIdent(parts.get(1)), DataTypes.STRING);
                        }
                        return new Field(t2, new ColumnIdent(parts.get(1)), DataTypes.STRING);
                    }

                    @Override
                    public Field resolveField(QualifiedName qualifiedName, @Nullable List<String> path, boolean forWrite) {
                        return null;
                    }
                }
        );
    }

    private QuerySplitter.SplitQueries split(AnalyzedRelation relation, String expression) {
        Expression parsedExpression = SqlParser.createExpression(expression);
        Symbol query = expressionAnalyzer.convert(parsedExpression, expressionAnalysisCtx);
        return QuerySplitter.splitForRelation(relation, query);
    }

    @Test
    public void testSplitNotOneRelation() throws Exception {
        QuerySplitter.SplitQueries splitQueries = split(t1, "not t1.id = 1");
        assertLiteralSymbol(splitQueries.remainingQuery(), true);
        assertThat(splitQueries.relationQuery(), isFunction("op_not"));

        splitQueries = split(t2, "not t1.id = 1");
        assertThat(splitQueries.remainingQuery(), isFunction("op_not"));
        assertThat(splitQueries.relationQuery(), nullValue());
    }

    @Test
    public void testSplitNotOneRelationAndOneRelation() throws Exception {
        QuerySplitter.SplitQueries splitQueries = split(t1, "not (t1.id = 1 and t1.id)");
        assertLiteralSymbol(splitQueries.remainingQuery(), true);
        assertThat(splitQueries.relationQuery(), isFunction("op_not"));
    }

    @Test
    public void testSplitNotOneRelationOrOneRelation() throws Exception {
        QuerySplitter.SplitQueries splitQueries = split(t1, "not (t1.id = 1 or t1.id)");
        assertLiteralSymbol(splitQueries.remainingQuery(), true);
        assertThat(splitQueries.relationQuery(), isFunction("op_not"));
    }

    @Test
    public void testSplitNotOneRelationOrAnotherRelation() throws Exception {
        QuerySplitter.SplitQueries splitQueries = split(t1, "not (t1.id = 1 or t2.id)");
        assertThat(splitQueries.remainingQuery(), isFunction("op_not"));
        assertThat(splitQueries.relationQuery(), nullValue());
    }
}