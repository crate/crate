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

package io.crate.analyze.where;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.PartitionName;
import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.Analyzer;
import io.crate.analyze.DeleteAnalyzedStatement;
import io.crate.analyze.UpdateAnalyzedStatement;
import io.crate.analyze.relations.TableRelation;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.operator.OperatorModule;
import io.crate.planner.RowGranularity;
import io.crate.sql.parser.SqlParser;
import io.crate.testing.MockedClusterServiceModule;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WhereClauseAnalyzerTest {

    private Analyzer analyzer;
    private AnalysisMetaData analysisMetaData;

    @Before
    public void setUp() throws Exception {
        Injector injector = new ModulesBuilder()
                .add(new MockedClusterServiceModule())
                .add(new OperatorModule())
                .add(new TestMetaDataModule()).createInjector();
        analyzer = injector.getInstance(Analyzer.class);
        analysisMetaData = injector.getInstance(AnalysisMetaData.class);
    }

    static final Routing twoNodeRouting = new Routing(ImmutableMap.<String, Map<String, Set<Integer>>>builder()
            .put("nodeOne", ImmutableMap.<String, Set<Integer>>of("t1", ImmutableSet.of(1, 2)))
            .put("nodeTow", ImmutableMap.<String, Set<Integer>>of("t1", ImmutableSet.of(3, 4)))
            .build());

    static class TestMetaDataModule extends MetaDataModule {
        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.name()).thenReturn(DocSchemaInfo.NAME);
            when(schemaInfo.getTableInfo("t")).thenReturn(
                    TestingTableInfo.builder(new TableIdent("doc", "t"), RowGranularity.DOC, twoNodeRouting)
                            .add("id", DataTypes.STRING, null)
                            .addPrimaryKey("id")
                            .build());
            when(schemaInfo.getTableInfo("p")).thenReturn(
                    TestingTableInfo.builder(new TableIdent("doc", "p"), RowGranularity.DOC, twoNodeRouting)
                            .add("date", DataTypes.TIMESTAMP, null, true)
                            .addPartitions(
                                    new PartitionName("p", Arrays.asList(new BytesRef("1395874800000"))).stringValue(),
                                    new PartitionName("p", Arrays.asList(new BytesRef("1395961200000"))).stringValue(),
                                    new PartitionName("p", new ArrayList<BytesRef>() {{add(null);}}).stringValue())
                    .build());
            schemaBinder.addBinding(DocSchemaInfo.NAME).toInstance(schemaInfo);
        }
    }

    private DeleteAnalyzedStatement analyzeDelete(String stmt, Object[][] bulkArgs) {
        return (DeleteAnalyzedStatement) analyzer.analyze(SqlParser.createStatement(stmt), new Object[0], bulkArgs).analyzedStatement();
    }

    private DeleteAnalyzedStatement analyzeDelete(String stmt) {
        return analyzeDelete(stmt, new Object[0][]);
    }

    private UpdateAnalyzedStatement analyzeUpdate(String stmt) {
        return (UpdateAnalyzedStatement) analyzer.analyze(
                SqlParser.createStatement(stmt), new Object[0], new Object[0][]).analyzedStatement();
    }

    @Test
    public void testWhereSinglePKColumnEq() throws Exception {
        DeleteAnalyzedStatement statement = analyzeDelete("delete from t where id = ?", new Object[][]{
                new Object[]{1},
                new Object[]{2},
                new Object[]{3},
        });
        TableInfo tableInfo = ((TableRelation) statement.analyzedRelation()).tableInfo();
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableInfo);

        assertThat(whereClauseAnalyzer.analyze(statement.whereClauses().get(0)).ids().get(0), is("1"));
        assertThat(whereClauseAnalyzer.analyze(statement.whereClauses().get(1)).ids().get(0), is("2"));
        assertThat(whereClauseAnalyzer.analyze(statement.whereClauses().get(2)).ids().get(0), is("3"));
    }

    @Test
    public void testWherePartitionedByColumn() throws Exception {
        DeleteAnalyzedStatement statement = analyzeDelete("delete from p where date = 1395874800000");
        TableInfo tableInfo = ((TableRelation) statement.analyzedRelation()).tableInfo();
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableInfo);
        WhereClauseContext ctx = whereClauseAnalyzer.analyze(statement.whereClauses().get(0));

        assertThat(ctx.whereClause().hasQuery(), is(false));
        assertThat(ctx.whereClause().noMatch(), is(false));
        assertThat(ctx.whereClause().partitions(),
                Matchers.contains(new PartitionName("p", Arrays.asList(new BytesRef("1395874800000"))).stringValue()));
    }

    @Test
    public void testUpdateWithVersionZeroIsNoMatch() throws Exception {
        UpdateAnalyzedStatement updateAnalyzedStatement = analyzeUpdate("update t set awesome = true where name = 'Ford' and _version = 0");
        TableInfo tableInfo = ((TableRelation) updateAnalyzedStatement.sourceRelation()).tableInfo();
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableInfo);
        assertThat(updateAnalyzedStatement.nestedStatements().get(0).whereClause().noMatch(), is(false));

        WhereClauseContext ctx = whereClauseAnalyzer.analyze(updateAnalyzedStatement.nestedStatements().get(0).whereClause());
        assertThat(ctx.whereClause().noMatch(), is(true));
    }

    @Test
    public void testUpdateWherePartitionedByColumn() throws Exception {
        UpdateAnalyzedStatement updateAnalyzedStatement = analyzeUpdate("update p set id = 2 where date = 1395874800000");
        UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalyzedStatement = updateAnalyzedStatement.nestedStatements().get(0);

        assertThat(nestedAnalyzedStatement.whereClause().hasQuery(), is(true));
        assertThat(nestedAnalyzedStatement.whereClause().noMatch(), is(false));

        TableInfo tableInfo = ((TableRelation) updateAnalyzedStatement.sourceRelation()).tableInfo();
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableInfo);
        WhereClauseContext context = whereClauseAnalyzer.analyze(nestedAnalyzedStatement.whereClause());

        assertThat(context.whereClause().hasQuery(), is(false));
        assertThat(context.whereClause().noMatch(), is(false));

        assertEquals(ImmutableList.of(
                        new PartitionName("p", Arrays.asList(new BytesRef("1395874800000"))).stringValue()),
                context.whereClause().partitions()
        );
    }
}