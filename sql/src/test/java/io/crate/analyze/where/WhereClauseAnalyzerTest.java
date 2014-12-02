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
import com.google.common.collect.Iterables;
import io.crate.PartitionName;
import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.RowGranularity;
import io.crate.sql.parser.SqlParser;
import io.crate.testing.MockedClusterServiceModule;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.AbstractRandomizedTest;
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

import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WhereClauseAnalyzerTest extends AbstractRandomizedTest {

    private Analyzer analyzer;
    private AnalysisMetaData ctxMetaData;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Injector injector = new ModulesBuilder()
                .add(new MockedClusterServiceModule())
                .add(new PredicateModule())
                .add(new OperatorModule())
                .add(new ScalarFunctionModule())
                .add(new MetaDataSysModule())
                .add(new TestMetaDataModule()).createInjector();
        analyzer = injector.getInstance(Analyzer.class);
        ctxMetaData = injector.getInstance(AnalysisMetaData.class);
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
            when(schemaInfo.getTableInfo("users")).thenReturn(
                    TestingTableInfo.builder(new TableIdent("doc", "users"), RowGranularity.DOC, twoNodeRouting)
                            .add("id", DataTypes.STRING, null)
                            .add("name", DataTypes.STRING, null)
                            .addPrimaryKey("id")
                            .clusteredBy("id")
                            .build());
            when(schemaInfo.getTableInfo("parted")).thenReturn(
                    TestingTableInfo.builder(new TableIdent("doc", "parted"), RowGranularity.DOC, twoNodeRouting)
                            .add("id", DataTypes.INTEGER, null)
                            .add("name", DataTypes.STRING, null)
                            .add("date", DataTypes.TIMESTAMP, null, true)
                            .addPartitions(
                                    new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).stringValue(),
                                    new PartitionName("parted", Arrays.asList(new BytesRef("1395961200000"))).stringValue(),
                                    new PartitionName("parted", new ArrayList<BytesRef>() {{
                                        add(null);
                                    }}).stringValue())
                    .build());
            when(schemaInfo.getTableInfo("bystring")).thenReturn(
                    TestingTableInfo.builder(new TableIdent("doc", "bystring"), RowGranularity.DOC, twoNodeRouting)
                            .add("name", DataTypes.STRING, null)
                            .add("score", DataTypes.DOUBLE, null)
                            .addPrimaryKey("name")
                            .clusteredBy("name")
                            .build());
            when(schemaInfo.getTableInfo("users_multi_pk")).thenReturn(
                TestingTableInfo.builder(new TableIdent("doc", "users_multi_pk"), RowGranularity.DOC, twoNodeRouting)
                        .add("id", DataTypes.LONG, null)
                        .add("name", DataTypes.STRING, null)
                        .add("details", DataTypes.OBJECT, null)
                        .add("awesome", DataTypes.BOOLEAN, null)
                        .add("friends", new ArrayType(DataTypes.OBJECT), null, ColumnPolicy.DYNAMIC)
                        .addPrimaryKey("id")
                        .addPrimaryKey("name")
                        .clusteredBy("id")
                        .build());
            when(schemaInfo.getTableInfo("users_clustered_by_only")).thenReturn(
                TestingTableInfo.builder(new TableIdent("doc", "users_clustered_by_only"), RowGranularity.DOC, twoNodeRouting)
                    .add("id", DataTypes.LONG, null)
                    .add("name", DataTypes.STRING, null)
                    .add("details", DataTypes.OBJECT, null)
                    .add("awesome", DataTypes.BOOLEAN, null)
                    .add("friends", new ArrayType(DataTypes.OBJECT), null, ColumnPolicy.DYNAMIC)
                    .clusteredBy("id")
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

    private WhereClauseContext analyzeSelectWhere(String stmt) {
        SelectAnalyzedStatement statement = (SelectAnalyzedStatement) analyzer.analyze(
                SqlParser.createStatement(stmt), new Object[0], new Object[0][]).analyzedStatement();
        assertThat(statement.sources().size(), is(1));
        AnalyzedRelation sourceRelation = Iterables.getOnlyElement(statement.sources().values());
        assertThat(sourceRelation, instanceOf(TableRelation.class));
        TableInfo tableInfo = ((TableRelation) sourceRelation).tableInfo();
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(ctxMetaData, tableInfo);
        return whereClauseAnalyzer.analyze(statement.whereClause());
    }

    @Test
    public void testWhereSinglePKColumnEq() throws Exception {
        DeleteAnalyzedStatement statement = analyzeDelete("delete from users where id = ?", new Object[][]{
                new Object[]{1},
                new Object[]{2},
                new Object[]{3},
        });
        TableInfo tableInfo = ((TableRelation) statement.analyzedRelation()).tableInfo();
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(ctxMetaData, tableInfo);

        assertThat(whereClauseAnalyzer.analyze(statement.whereClauses().get(0)).ids().get(0), is("1"));
        assertThat(whereClauseAnalyzer.analyze(statement.whereClauses().get(1)).ids().get(0), is("2"));
        assertThat(whereClauseAnalyzer.analyze(statement.whereClauses().get(2)).ids().get(0), is("3"));
    }

    @Test
    public void testWherePartitionedByColumn() throws Exception {
        DeleteAnalyzedStatement statement = analyzeDelete("delete from parted where date = 1395874800000");
        TableInfo tableInfo = ((TableRelation) statement.analyzedRelation()).tableInfo();
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(ctxMetaData, tableInfo);
        WhereClauseContext ctx = whereClauseAnalyzer.analyze(statement.whereClauses().get(0));

        assertThat(ctx.whereClause().hasQuery(), is(false));
        assertThat(ctx.whereClause().noMatch(), is(false));
        assertThat(ctx.whereClause().partitions(),
                Matchers.contains(new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).stringValue()));
    }

    @Test
    public void testUpdateWithVersionZeroIsNoMatch() throws Exception {
        UpdateAnalyzedStatement updateAnalyzedStatement = analyzeUpdate("update users set awesome = true where name = 'Ford' and _version = 0");
        TableInfo tableInfo = ((TableRelation) updateAnalyzedStatement.sourceRelation()).tableInfo();
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(ctxMetaData, tableInfo);
        assertThat(updateAnalyzedStatement.nestedStatements().get(0).whereClause().noMatch(), is(false));

        WhereClauseContext ctx = whereClauseAnalyzer.analyze(updateAnalyzedStatement.nestedStatements().get(0).whereClause());
        assertThat(ctx.whereClause().noMatch(), is(true));
    }

    @Test
    public void testUpdateWherePartitionedByColumn() throws Exception {
        UpdateAnalyzedStatement updateAnalyzedStatement = analyzeUpdate("update parted set id = 2 where date = 1395874800000");
        UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalyzedStatement = updateAnalyzedStatement.nestedStatements().get(0);

        assertThat(nestedAnalyzedStatement.whereClause().hasQuery(), is(true));
        assertThat(nestedAnalyzedStatement.whereClause().noMatch(), is(false));

        TableInfo tableInfo = ((TableRelation) updateAnalyzedStatement.sourceRelation()).tableInfo();
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(ctxMetaData, tableInfo);
        WhereClauseContext context = whereClauseAnalyzer.analyze(nestedAnalyzedStatement.whereClause());

        assertThat(context.whereClause().hasQuery(), is(false));
        assertThat(context.whereClause().noMatch(), is(false));

        assertEquals(ImmutableList.of(
                        new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).stringValue()),
                context.whereClause().partitions()
        );
    }

    @Test
    public void testClusteredByValueContainsComma() throws Exception {
        WhereClauseContext whereClauseContext = analyzeSelectWhere("select * from bystring where name = 'a,b,c'");
        assertThat(whereClauseContext.whereClause().clusteredBy().get(), is("a,b,c"));
        assertThat(whereClauseContext.ids().size(), is(1));
        assertThat(whereClauseContext.ids().get(0), is("a,b,c"));
    }

    @Test
    public void testEmptyClusteredByValue() throws Exception {
        WhereClauseContext ctx = analyzeSelectWhere("select * from bystring where name = ''");
        assertThat(ctx.whereClause().clusteredBy().get(), is(""));
        assertThat(ctx.ids().size(), is(1));
        assertThat(ctx.ids().get(0), is(""));
    }

    @Test
    public void testClusteredBy() throws Exception {
        WhereClauseContext ctx = analyzeSelectWhere("select name from users where id=1");
        assertEquals(ImmutableList.of("1"), ctx.routingValues());
        assertEquals("1", ctx.whereClause().clusteredBy().get());

        ctx = analyzeSelectWhere("select name from users where id=1 or id=2");
        assertThat(ctx.routingValues(), containsInAnyOrder("1", "2"));
        assertThat(ctx.ids(), equalTo(ctx.routingValues()));
        assertFalse(ctx.whereClause().clusteredBy().isPresent());
    }


    @Test
    public void testClusteredByOnly() throws Exception {
        WhereClauseContext ctx = analyzeSelectWhere("select name from users_clustered_by_only where id=1");
        assertEquals(ImmutableList.of(), ctx.ids());
        assertEquals(ImmutableList.of("1"), ctx.routingValues());
        assertEquals("1", ctx.whereClause().clusteredBy().get());

        ctx = analyzeSelectWhere("select name from users_clustered_by_only where id=1 or id=2");
        assertEquals(ImmutableList.of(), ctx.ids());
        assertEquals(ImmutableList.of(), ctx.routingValues());
        assertFalse(ctx.whereClause().clusteredBy().isPresent());

        ctx = analyzeSelectWhere("select name from users_clustered_by_only where id=1 and id=2");
        assertEquals(ImmutableList.of(), ctx.ids());
        assertEquals(ImmutableList.of(), ctx.routingValues());
        assertFalse(ctx.whereClause().clusteredBy().isPresent());
    }

    @Test
    public void testCompositePrimaryKey() throws Exception {
        WhereClauseContext ctx = analyzeSelectWhere("select name from users_multi_pk where id=1");
        assertEquals(ImmutableList.of(), ctx.ids());
        assertEquals(ImmutableList.of(), ctx.routingValues());
        assertEquals("1", ctx.whereClause().clusteredBy().get());

        ctx = analyzeSelectWhere("select name from users_multi_pk where id=1 and name='Douglas'");
        assertEquals(ImmutableList.of("AgExB0RvdWdsYXM="), ctx.ids());
        assertEquals(ImmutableList.of("1"), ctx.routingValues());
        assertEquals("1", ctx.whereClause().clusteredBy().get());

        ctx = analyzeSelectWhere("select name from users_multi_pk where id=1 or id=2 and name='Douglas'");
        assertEquals(ImmutableList.of(), ctx.ids());
        assertEquals(ImmutableList.of(), ctx.routingValues());
        assertFalse(ctx.whereClause().clusteredBy().isPresent());

        ctx = analyzeSelectWhere("select name from users_multi_pk where id=1 and name='Douglas' or name='Arthur'");
        assertEquals(ImmutableList.of(), ctx.ids());
        assertEquals(ImmutableList.of(), ctx.routingValues());
        assertFalse(ctx.whereClause().clusteredBy().isPresent());
    }

    @Test
    public void testPrimaryKeyAndVersion() throws Exception {
        WhereClauseContext ctx = analyzeSelectWhere(
                "select name from users where id = 2 and \"_version\" = 1");
        assertEquals(ImmutableList.of("2"), ctx.ids());
        assertEquals(ImmutableList.of("2"), ctx.routingValues());
        assertThat(ctx.whereClause().version().get(), is(1L));
    }

    @Test
    public void testMultiplePrimaryKeys() throws Exception {
        WhereClauseContext ctx = analyzeSelectWhere(
                "select name from users where id = 2 or id = 1");

        assertThat(ctx.ids(), containsInAnyOrder("1", "2"));
        assertThat(ctx.routingValues(), containsInAnyOrder("1", "2"));
    }

    @Test
    public void testMultiplePrimaryKeysAndInvalidColumn() throws Exception {
        WhereClauseContext ctx = analyzeSelectWhere(
                "select name from users where id = 2 or id = 1 and name = 'foo'");
        assertEquals(0, ctx.ids().size());
    }

    @Test
    public void testNotEqualsDoesntMatchPrimaryKey() throws Exception {
        WhereClauseContext ctx = analyzeSelectWhere("select name from users where id != 1");
        assertEquals(0, ctx.ids().size());
        assertEquals(0, ctx.routingValues().size());
    }

    @Test
    public void testMultipleCompoundPrimaryKeys() throws Exception {
        WhereClauseContext ctx = analyzeSelectWhere(
                "select * from sys.shards where (schema_name='doc' and id = 1 and table_name = 'foo' and partition_ident='') " +
                        "or (schema_name='doc' and id = 2 and table_name = 'bla' and partition_ident='')");
        assertEquals(ImmutableList.of("BANkb2MDZm9vATEA", "BANkb2MDYmxhATIA"), ctx.ids());
        assertEquals(ImmutableList.of("BANkb2MDZm9vATEA", "BANkb2MDYmxhATIA"), ctx.routingValues());
        assertFalse(ctx.whereClause().clusteredBy().isPresent());

        ctx = analyzeSelectWhere(
                "select * from sys.shards where (schema_name='doc' and id = 1 and table_name = 'foo') " +
                        "or (schema_name='doc' and id = 2 and table_name = 'bla') or id = 1");
        assertEquals(ImmutableList.of(), ctx.ids());
        assertEquals(ImmutableList.of(), ctx.routingValues());
        assertFalse(ctx.whereClause().clusteredBy().isPresent());
    }

    @Test
    public void test1ColPrimaryKey() throws Exception {
        WhereClauseContext ctx = analyzeSelectWhere("select name from sys.nodes where id='jalla'");
        assertEquals(ImmutableList.of("jalla"), ctx.ids());
        assertEquals(ImmutableList.of("jalla"), ctx.routingValues());

        ctx = analyzeSelectWhere("select name from sys.nodes where 'jalla'=id");
        assertEquals(ImmutableList.of("jalla"), ctx.ids());

        ctx = analyzeSelectWhere("select name from sys.nodes where id='jalla' and id='jalla'");
        assertEquals(ImmutableList.of("jalla"), ctx.ids());

        ctx = analyzeSelectWhere("select name from sys.nodes where id='jalla' and (id='jalla' or 1=1)");
        assertEquals(ImmutableList.of("jalla"), ctx.ids());

        // a no match results in undefined key literals, since those are ambiguous
        ctx = analyzeSelectWhere("select name from sys.nodes where id='jalla' and id='kelle'");
        assertEquals(ImmutableList.of(), ctx.ids());
        assertTrue(ctx.whereClause().noMatch());

        ctx = analyzeSelectWhere("select name from sys.nodes where id='jalla' or name = 'something'");
        assertEquals(ImmutableList.of(), ctx.ids());
        assertFalse(ctx.whereClause().noMatch());

        ctx = analyzeSelectWhere("select name from sys.nodes where name = 'something'");
        assertEquals(ImmutableList.of(), ctx.ids());
        assertFalse(ctx.whereClause().noMatch());

    }

    @Test
    public void test3ColPrimaryKey() throws Exception {
        WhereClauseContext ctx = analyzeSelectWhere("select id from sys.shards where id=1 and table_name='jalla' and schema_name='doc' and partition_ident=''");
        // base64 encoded versions of Streamable of ["doc","jalla","1"]
        assertEquals(ImmutableList.of("BANkb2MFamFsbGEBMQA="), ctx.ids());
        assertEquals(ImmutableList.of("BANkb2MFamFsbGEBMQA="), ctx.routingValues());
        assertFalse(ctx.whereClause().noMatch());

        ctx = analyzeSelectWhere("select id from sys.shards where id=1 and table_name='jalla' and id=1 and schema_name='doc' and partition_ident=''");
        // base64 encoded versions of Streamable of ["doc","jalla","1"]
        assertEquals(ImmutableList.of("BANkb2MFamFsbGEBMQA="), ctx.ids());
        assertEquals(ImmutableList.of("BANkb2MFamFsbGEBMQA="), ctx.routingValues());
        assertFalse(ctx.whereClause().noMatch());


        ctx = analyzeSelectWhere("select id from sys.shards where id=1");
        assertEquals(ImmutableList.of(), ctx.ids());
        assertFalse(ctx.whereClause().noMatch());

        ctx = analyzeSelectWhere("select id from sys.shards where id=1 and schema_name='doc' and table_name='jalla' and id=2 and partition_ident=''");
        assertEquals(ImmutableList.of(), ctx.ids());
        assertTrue(ctx.whereClause().noMatch());
    }

    @Test
    public void test1ColPrimaryKeySetLiteralDiffMatches() throws Exception {
        WhereClauseContext ctx = analyzeSelectWhere(
                "select name from sys.nodes where id in ('jalla', 'kelle') and id in ('jalla', 'something')");
        assertFalse(ctx.whereClause().noMatch());
        assertEquals(1, ctx.ids().size());
        assertEquals("jalla", ctx.ids().get(0));
    }


    @Test
    public void test1ColPrimaryKeySetLiteral() throws Exception {
        WhereClauseContext ctx = analyzeSelectWhere("select name from sys.nodes where id in ('jalla', 'kelle')");
        assertFalse(ctx.whereClause().noMatch());
        assertEquals(2, ctx.ids().size());
        assertThat(ctx.ids(), containsInAnyOrder("jalla", "kelle"));
    }

    @Test
    public void test3ColPrimaryKeySetLiteral() throws Exception {
        WhereClauseContext ctx = analyzeSelectWhere("select id from sys.shards where id=1 and schema_name='doc' and table_name in ('jalla', 'kelle') and partition_ident=''");
        assertEquals(2, ctx.ids().size());
        // base64 encoded versions of Streamable of ["doc","jalla","1"] and ["doc","kelle","1"]

        assertThat(ctx.ids(), containsInAnyOrder("BANkb2MFamFsbGEBMQA=", "BANkb2MFa2VsbGUBMQA="));
        assertThat(ctx.routingValues(), containsInAnyOrder("BANkb2MFamFsbGEBMQA=", "BANkb2MFa2VsbGUBMQA="));
    }

    @Test
    public void testSelectFromPartitionedTable() throws Exception {
        String partition1 = new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).stringValue();
        String partition2 = new PartitionName("parted", Arrays.asList(new BytesRef("1395961200000"))).stringValue();
        String partition3 = new PartitionName("parted", new ArrayList<BytesRef>(){{add(null);}}).stringValue();

        WhereClauseContext ctx = analyzeSelectWhere("select id, name from parted where date = 1395874800000");
        assertEquals(ImmutableList.of(partition1), ctx.whereClause().partitions());
        assertFalse(ctx.whereClause().hasQuery());
        assertFalse(ctx.whereClause().noMatch());

        ctx = analyzeSelectWhere("select id, name from parted where date = 1395874800000 " +
                "and substr(name, 0, 4) = 'this'");
        assertEquals(ImmutableList.of(partition1), ctx.whereClause().partitions());
        assertThat(ctx.whereClause().hasQuery(), is(true));
        assertThat(ctx.whereClause().noMatch(), is(false));

        ctx = analyzeSelectWhere("select id, name from parted where date >= 1395874800000");
        assertThat(ctx.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(ctx.whereClause().hasQuery());
        assertFalse(ctx.whereClause().noMatch());

        ctx = analyzeSelectWhere("select id, name from parted where date < 1395874800000");
        assertEquals(ImmutableList.of(), ctx.whereClause().partitions());
        assertTrue(ctx.whereClause().noMatch());

        ctx = analyzeSelectWhere("select id, name from parted where date = 1395874800000 and date = 1395961200000");
        assertEquals(ImmutableList.of(), ctx.whereClause().partitions());
        assertTrue(ctx.whereClause().noMatch());

        ctx = analyzeSelectWhere("select id, name from parted where date = 1395874800000 or date = 1395961200000");
        assertThat(ctx.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(ctx.whereClause().hasQuery());
        assertFalse(ctx.whereClause().noMatch());

        ctx = analyzeSelectWhere("select id, name from parted where date < 1395874800000 or date > 1395874800000");
        assertEquals(ImmutableList.of(partition2), ctx.whereClause().partitions());
        assertFalse(ctx.whereClause().hasQuery());
        assertFalse(ctx.whereClause().noMatch());

        ctx = analyzeSelectWhere("select id, name from parted where date in (1395874800000, 1395961200000)");
        assertThat(ctx.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(ctx.whereClause().hasQuery());
        assertFalse(ctx.whereClause().noMatch());

        ctx = analyzeSelectWhere("select id, name from parted where date in (1395874800000, 1395961200000) and id = 1");
        assertThat(ctx.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertTrue(ctx.whereClause().hasQuery());
        assertFalse(ctx.whereClause().noMatch());

        /**
         *
         * col = 'undefined' -> null as col doesn't exist
         * ->
         *  not (true  and null) -> not (null)  -> no match
         *  not (null  and null) -> not (null)  -> no match
         *  not (false and null) -> not (false) -> match
         */
        ctx = analyzeSelectWhere("select id, name from parted where not (date = 1395874800000 and obj['col'] = 'undefined')");
        assertThat(ctx.whereClause().partitions(), containsInAnyOrder(partition2));
        assertThat(ctx.whereClause().hasQuery(), is(false));
        assertThat(ctx.whereClause().noMatch(), is(false));

        ctx = analyzeSelectWhere("select id, name from parted where date in (1395874800000) or date in (1395961200000)");
        assertThat(ctx.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(ctx.whereClause().hasQuery());
        assertFalse(ctx.whereClause().noMatch());

        ctx = analyzeSelectWhere("select id, name from parted where date = 1395961200000 and id = 1");
        assertEquals(ImmutableList.of(partition2), ctx.whereClause().partitions());
        assertTrue(ctx.whereClause().hasQuery());
        assertFalse(ctx.whereClause().noMatch());

        ctx = analyzeSelectWhere("select id, name from parted where (date =1395874800000 or date = 1395961200000) and id = 1");
        assertThat(ctx.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertTrue(ctx.whereClause().hasQuery());
        assertFalse(ctx.whereClause().noMatch());

        ctx = analyzeSelectWhere("select id, name from parted where date = 1395874800000 and id is null");
        assertEquals(ImmutableList.of(partition1), ctx.whereClause().partitions());
        assertTrue(ctx.whereClause().hasQuery());
        assertFalse(ctx.whereClause().noMatch());

        ctx = analyzeSelectWhere("select id, name from parted where date is null and id = 1");
        assertEquals(ImmutableList.of(partition3), ctx.whereClause().partitions());
        assertTrue(ctx.whereClause().hasQuery());
        assertFalse(ctx.whereClause().noMatch());

        ctx = analyzeSelectWhere("select id, name from parted where 1395874700000 < date and date < 1395961200001");
        assertThat(ctx.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(ctx.whereClause().hasQuery());
        assertFalse(ctx.whereClause().noMatch());

        ctx = analyzeSelectWhere("select id, name from parted where '2014-03-16T22:58:20' < date and date < '2014-03-27T23:00:01'");
        assertThat(ctx.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(ctx.whereClause().hasQuery());
        assertFalse(ctx.whereClause().noMatch());
    }

    @Test
    public void testSelectFromPartitionedTableUnsupported() throws Exception {
        // these queries won't work because we would have to execute 2 separate ESSearch tasks
        // and merge results which is not supported right now and maybe never will be
        try {
            analyzeSelectWhere("select id, name from parted where date = 1395961200000 or id = 1");
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            assertThat(e.getMessage(),
                is("logical conjunction of the conditions in the WHERE clause which involve " +
                    "partitioned columns led to a query that can't be executed."));
        }

        try {
            analyzeSelectWhere("select id, name from parted where id = 1 or date = 1395961200000");
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            assertThat(e.getMessage(),
                is("logical conjunction of the conditions in the WHERE clause which involve " +
                    "partitioned columns led to a query that can't be executed."));
        }
    }
}