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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.*;
import io.crate.analyze.relations.TableRelation;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.metadata.*;
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

import static io.crate.testing.TestingHelpers.isLiteral;
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

    static final Routing twoNodeRouting = new Routing(TreeMapBuilder.<String, Map<String, Set<Integer>>>newMapBuilder()
            .put("nodeOne", TreeMapBuilder.<String, Set<Integer>>newMapBuilder().put("t1", ImmutableSet.of(1, 2)).map())
            .put("nodeTow", TreeMapBuilder.<String, Set<Integer>>newMapBuilder().put("t1", ImmutableSet.of(3, 4)).map())
            .map());

    static class TestMetaDataModule extends MetaDataModule {
        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.name()).thenReturn(ReferenceInfos.DEFAULT_SCHEMA_NAME);
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
                            .add("obj", DataTypes.OBJECT, null, ColumnPolicy.IGNORED)
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
            schemaBinder.addBinding(ReferenceInfos.DEFAULT_SCHEMA_NAME).toInstance(schemaInfo);
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

    private WhereClause analyzeSelectWhere(String stmt) {
        SelectAnalyzedStatement statement = (SelectAnalyzedStatement) analyzer.analyze(
                SqlParser.createStatement(stmt), new Object[0], new Object[0][]).analyzedStatement();
        QueriedTable sourceRelation = (QueriedTable) statement.relation();
        TableRelation tableRelation = sourceRelation.tableRelation();
        TableInfo tableInfo = tableRelation.tableInfo();
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(ctxMetaData, tableRelation);
        return whereClauseAnalyzer.analyze(statement.relation().querySpec().where());
    }

    @Test
    public void testWhereSinglePKColumnEq() throws Exception {
        DeleteAnalyzedStatement statement = analyzeDelete("delete from users where id = ?", new Object[][]{
                new Object[]{1},
                new Object[]{2},
                new Object[]{3},
        });
        TableRelation tableRelation = (TableRelation) statement.analyzedRelation();
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(ctxMetaData, tableRelation);

        assertThat(whereClauseAnalyzer.analyze(statement.whereClauses().get(0)).primaryKeys().get().get(0).stringValue(), is("1"));
        assertThat(whereClauseAnalyzer.analyze(statement.whereClauses().get(1)).primaryKeys().get().get(0).stringValue(), is("2"));
        assertThat(whereClauseAnalyzer.analyze(statement.whereClauses().get(2)).primaryKeys().get().get(0).stringValue(), is("3"));
    }

    @Test
    public void testWherePartitionedByColumn() throws Exception {
        DeleteAnalyzedStatement statement = analyzeDelete("delete from parted where date = 1395874800000");
        TableRelation tableRelation = (TableRelation) statement.analyzedRelation();
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(ctxMetaData, tableRelation);
        WhereClause whereClause = whereClauseAnalyzer.analyze(statement.whereClauses().get(0));

        assertThat(whereClause.hasQuery(), is(false));
        assertThat(whereClause.noMatch(), is(false));
        assertThat(whereClause.partitions(),
                Matchers.contains(new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).stringValue()));
    }

    @Test
    public void testUpdateWithVersionZeroIsNoMatch() throws Exception {
        UpdateAnalyzedStatement updateAnalyzedStatement = analyzeUpdate("update users set awesome = true where name = 'Ford' and _version = 0");
        TableRelation tableRelation = (TableRelation) updateAnalyzedStatement.sourceRelation();
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(ctxMetaData, tableRelation);
        assertThat(updateAnalyzedStatement.nestedStatements().get(0).whereClause().noMatch(), is(false));

        WhereClause whereClause = whereClauseAnalyzer.analyze(updateAnalyzedStatement.nestedStatements().get(0).whereClause());
        assertThat(whereClause.noMatch(), is(true));
    }

    @Test
    public void testUpdateWherePartitionedByColumn() throws Exception {
        UpdateAnalyzedStatement updateAnalyzedStatement = analyzeUpdate("update parted set id = 2 where date = 1395874800000");
        UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalyzedStatement = updateAnalyzedStatement.nestedStatements().get(0);

        assertThat(nestedAnalyzedStatement.whereClause().hasQuery(), is(true));
        assertThat(nestedAnalyzedStatement.whereClause().noMatch(), is(false));

        TableRelation tableRelation = (TableRelation) updateAnalyzedStatement.sourceRelation();
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(ctxMetaData, tableRelation);
        WhereClause whereClause = whereClauseAnalyzer.analyze(nestedAnalyzedStatement.whereClause());

        assertThat(whereClause.hasQuery(), is(false));
        assertThat(whereClause.noMatch(), is(false));

        assertEquals(ImmutableList.of(
                        new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).stringValue()),
                whereClause.partitions()
        );
    }

    @Test
    public void testClusteredByValueContainsComma() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select * from bystring where name = 'a,b,c'");
        assertThat(whereClause.clusteredBy().get(), contains(isLiteral("a,b,c", DataTypes.STRING)));
        assertThat(whereClause.primaryKeys().get().size(), is(1));
        assertThat(whereClause.primaryKeys().get().get(0).stringValue(), is("a,b,c"));
    }

    @Test
    public void testEmptyClusteredByValue() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select * from bystring where name = ''");
        assertTrue(whereClause.clusteredBy().isPresent());
        assertTrue(whereClause.primaryKeys().isPresent());
    }

    @Test
    public void testClusteredBy() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select name from users where id=1");
        assertThat(whereClause.primaryKeys().get().get(0).stringValue(), is("1"));
        assertThat(whereClause.clusteredBy().get(), contains(isLiteral("1")));

        whereClause = analyzeSelectWhere("select name from users where id=1 or id=2");

        assertThat(whereClause.primaryKeys().get().size(),is(2));
        assertThat(whereClause.primaryKeys().get(), containsInAnyOrder(hasToString("1"), hasToString("2")));

        assertThat(whereClause.clusteredBy().get(), containsInAnyOrder(isLiteral("1"), isLiteral("2")));

    }


    @Test
    public void testClusteredByOnly() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select name from users_clustered_by_only where id=1");

        assertFalse(whereClause.primaryKeys().isPresent());
        assertThat(whereClause.clusteredBy().get(),  contains(isLiteral(1L)));

        whereClause = analyzeSelectWhere("select name from users_clustered_by_only where id=1 or id=2");
        assertFalse(whereClause.primaryKeys().isPresent());
        assertThat(whereClause.clusteredBy().get(), containsInAnyOrder(
                isLiteral(1L), isLiteral(2L)));


        // TODO: optimize this case: routing should be 3,4,5
        whereClause = analyzeSelectWhere("select name from users_clustered_by_only where id in (3,4,5)");
        assertFalse(whereClause.primaryKeys().isPresent());
        assertFalse(whereClause.clusteredBy().isPresent());


        // TODO: optimize this case: there are two routing values here, which are currently not set
        whereClause = analyzeSelectWhere("select name from users_clustered_by_only where id=1 and id=2");
        assertFalse(whereClause.primaryKeys().isPresent());
        assertFalse(whereClause.clusteredBy().isPresent());
    }

    @Test
    public void testCompositePrimaryKey() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select name from users_multi_pk where id=1");
        assertFalse(whereClause.primaryKeys().isPresent());
        assertThat(whereClause.clusteredBy().get(), contains(isLiteral(1L)));

        whereClause = analyzeSelectWhere("select name from users_multi_pk where id=1 and name='Douglas'");
        assertThat(whereClause.primaryKeys().get(), contains(hasToString("AgExB0RvdWdsYXM=")));
        assertThat(whereClause.clusteredBy().get(), contains(isLiteral(1L)));

        // TODO: optimize this case, since the routing values are 1,2
        whereClause = analyzeSelectWhere("select name from users_multi_pk where id=1 or id=2 and name='Douglas'");
        assertFalse(whereClause.primaryKeys().isPresent());
        assertFalse(whereClause.clusteredBy().isPresent());

        whereClause = analyzeSelectWhere("select name from users_multi_pk where id=1 and name='Douglas' or name='Arthur'");
        assertFalse(whereClause.primaryKeys().isPresent());
        assertFalse(whereClause.clusteredBy().isPresent());
    }

    @Test
    public void testPrimaryKeyAndVersion() throws Exception {
        WhereClause whereClause = analyzeSelectWhere(
                "select name from users where id = 2 and \"_version\" = 1");
        assertThat(whereClause.primaryKeys().get(), contains(hasToString("2")));
        assertThat(whereClause.version().get(), is(1L));
    }

    @Test
    public void testMultiplePrimaryKeys() throws Exception {
        WhereClause whereClause = analyzeSelectWhere(
                "select name from users where id = 2 or id = 1");
        assertThat(whereClause.primaryKeys().get(), containsInAnyOrder(hasToString("1"), hasToString("2")));
        assertThat(whereClause.clusteredBy().get(), containsInAnyOrder(isLiteral("1"), isLiteral("2")));
    }

    @Test
    public void testMultiplePrimaryKeysAndInvalidColumn() throws Exception {
        WhereClause whereClause = analyzeSelectWhere(
                "select name from users where id = 2 or id = 1 and name = 'foo'");
        assertFalse(whereClause.primaryKeys().isPresent());
    }

    @Test
    public void testNotEqualsDoesntMatchPrimaryKey() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select name from users where id != 1");
        assertFalse(whereClause.primaryKeys().isPresent());
        assertFalse(whereClause.clusteredBy().isPresent());
    }

    @Test
    public void testMultipleCompoundPrimaryKeys() throws Exception {
        WhereClause whereClause = analyzeSelectWhere(
                "select * from sys.shards where (schema_name='doc' and id = 1 and table_name = 'foo' and partition_ident='') " +
                        "or (schema_name='doc' and id = 2 and table_name = 'bla' and partition_ident='')");

        assertThat(whereClause.primaryKeys().get(), containsInAnyOrder(hasToString("BANkb2MDZm9vATEA"),
                hasToString("BANkb2MDYmxhATIA")));
        assertFalse(whereClause.clusteredBy().isPresent());

        whereClause = analyzeSelectWhere(
                "select * from sys.shards where (schema_name='doc' and id = 1 and table_name = 'foo') " +
                        "or (schema_name='doc' and id = 2 and table_name = 'bla') or id = 1");
        assertFalse(whereClause.primaryKeys().isPresent());
        assertFalse(whereClause.clusteredBy().isPresent());
    }

    @Test
    public void test1ColPrimaryKey() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select name from sys.nodes where id='jalla'");

        assertThat(whereClause.primaryKeys().get(), contains(hasToString("jalla")));

        whereClause = analyzeSelectWhere("select name from sys.nodes where 'jalla'=id");
        assertThat(whereClause.primaryKeys().get(), contains(hasToString("jalla")));

        whereClause = analyzeSelectWhere("select name from sys.nodes where id='jalla' and id='jalla'");
        assertThat(whereClause.primaryKeys().get(), contains(hasToString("jalla")));

        whereClause = analyzeSelectWhere("select name from sys.nodes where id='jalla' and (id='jalla' or 1=1)");
        assertThat(whereClause.primaryKeys().get(), contains(hasToString("jalla")));

        // a no match results in undefined key literals, since those are ambiguous
        whereClause = analyzeSelectWhere("select name from sys.nodes where id='jalla' and id='kelle'");
        assertFalse(whereClause.primaryKeys().isPresent());
        assertTrue(whereClause.noMatch());

        whereClause = analyzeSelectWhere("select name from sys.nodes where id='jalla' or name = 'something'");
        assertFalse(whereClause.primaryKeys().isPresent());
        assertFalse(whereClause.noMatch());

        whereClause = analyzeSelectWhere("select name from sys.nodes where name = 'something'");
        assertFalse(whereClause.primaryKeys().isPresent());
        assertFalse(whereClause.noMatch());

    }

    @Test
    public void test3ColPrimaryKey() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select id from sys.shards where id=1 and table_name='jalla' and schema_name='doc' and partition_ident=''");
        // base64 encoded versions of Streamable of ["doc","jalla","1"]
        assertThat(whereClause.primaryKeys().get(), contains(hasToString("BANkb2MFamFsbGEBMQA=")));
        assertFalse(whereClause.noMatch());

        whereClause = analyzeSelectWhere("select id from sys.shards where id=1 and table_name='jalla' and id=1 and schema_name='doc' and partition_ident=''");
        assertThat(whereClause.primaryKeys().get(), contains(hasToString("BANkb2MFamFsbGEBMQA=")));
        assertFalse(whereClause.noMatch());


        whereClause = analyzeSelectWhere("select id from sys.shards where id=1");
        assertFalse(whereClause.primaryKeys().isPresent());
        assertFalse(whereClause.noMatch());

        whereClause = analyzeSelectWhere("select id from sys.shards where id=1 and schema_name='doc' and table_name='jalla' and id=2 and partition_ident=''");
        assertFalse(whereClause.primaryKeys().isPresent());
        assertTrue(whereClause.noMatch());
    }

    @Test
    public void test1ColPrimaryKeySetLiteralDiffMatches() throws Exception {
        WhereClause whereClause = analyzeSelectWhere(
                "select name from sys.nodes where id in ('jalla', 'kelle') and id in ('jalla', 'something')");
        assertFalse(whereClause.noMatch());
        assertThat(whereClause.primaryKeys().get(), contains(hasToString("jalla")));
    }


    @Test
    public void test1ColPrimaryKeySetLiteral() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select name from sys.nodes where id in ('jalla', 'kelle')");
        assertFalse(whereClause.noMatch());
        assertThat(whereClause.primaryKeys().get(), containsInAnyOrder(hasToString("jalla"), hasToString("kelle")));
    }

    @Test
    public void test3ColPrimaryKeySetLiteral() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select id from sys.shards where id=1 and schema_name='doc' and table_name in ('jalla', 'kelle') and partition_ident=''");
        assertEquals(2, whereClause.primaryKeys().get().size());
        // base64 encoded versions of Streamable of ["doc","jalla","1"] and ["doc","kelle","1"]
        assertThat(whereClause.primaryKeys().get(), containsInAnyOrder(
                hasToString("BANkb2MFamFsbGEBMQA="), hasToString("BANkb2MFa2VsbGUBMQA=")));
    }

    @Test
    public void testSelectFromPartitionedTable() throws Exception {
        String partition1 = new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).stringValue();
        String partition2 = new PartitionName("parted", Arrays.asList(new BytesRef("1395961200000"))).stringValue();
        String partition3 = new PartitionName("parted", new ArrayList<BytesRef>(){{add(null);}}).stringValue();

        WhereClause whereClause = analyzeSelectWhere("select id, name from parted where date = 1395874800000");
        assertEquals(ImmutableList.of(partition1), whereClause.partitions());
        assertFalse(whereClause.hasQuery());
        assertFalse(whereClause.noMatch());

        whereClause = analyzeSelectWhere("select id, name from parted where date = 1395874800000 " +
                "and substr(name, 0, 4) = 'this'");
        assertEquals(ImmutableList.of(partition1), whereClause.partitions());
        assertThat(whereClause.hasQuery(), is(true));
        assertThat(whereClause.noMatch(), is(false));

        whereClause = analyzeSelectWhere("select id, name from parted where date >= 1395874800000");
        assertThat(whereClause.partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(whereClause.hasQuery());
        assertFalse(whereClause.noMatch());

        whereClause = analyzeSelectWhere("select id, name from parted where date < 1395874800000");
        assertEquals(ImmutableList.of(), whereClause.partitions());
        assertTrue(whereClause.noMatch());

        whereClause = analyzeSelectWhere("select id, name from parted where date = 1395874800000 and date = 1395961200000");
        assertEquals(ImmutableList.of(), whereClause.partitions());
        assertTrue(whereClause.noMatch());

        whereClause = analyzeSelectWhere("select id, name from parted where date = 1395874800000 or date = 1395961200000");
        assertThat(whereClause.partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(whereClause.hasQuery());
        assertFalse(whereClause.noMatch());

        whereClause = analyzeSelectWhere("select id, name from parted where date < 1395874800000 or date > 1395874800000");
        assertEquals(ImmutableList.of(partition2), whereClause.partitions());
        assertFalse(whereClause.hasQuery());
        assertFalse(whereClause.noMatch());

        whereClause = analyzeSelectWhere("select id, name from parted where date in (1395874800000, 1395961200000)");
        assertThat(whereClause.partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(whereClause.hasQuery());
        assertFalse(whereClause.noMatch());

        whereClause = analyzeSelectWhere("select id, name from parted where date in (1395874800000, 1395961200000) and id = 1");
        assertThat(whereClause.partitions(), containsInAnyOrder(partition1, partition2));
        assertTrue(whereClause.hasQuery());
        assertFalse(whereClause.noMatch());

        /**
         *
         * col = 'undefined' -> null as col doesn't exist
         * ->
         *  not (true  and null) -> not (null)  -> no match
         *  not (null  and null) -> not (null)  -> no match
         *  not (false and null) -> not (false) -> match
         */
        whereClause = analyzeSelectWhere("select id, name from parted where not (date = 1395874800000 and obj['col'] = 'undefined')");
        assertThat(whereClause.partitions(), containsInAnyOrder(partition2));
        assertThat(whereClause.hasQuery(), is(false));
        assertThat(whereClause.noMatch(), is(false));

        whereClause = analyzeSelectWhere("select id, name from parted where date in (1395874800000) or date in (1395961200000)");
        assertThat(whereClause.partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(whereClause.hasQuery());
        assertFalse(whereClause.noMatch());

        whereClause = analyzeSelectWhere("select id, name from parted where date = 1395961200000 and id = 1");
        assertEquals(ImmutableList.of(partition2), whereClause.partitions());
        assertTrue(whereClause.hasQuery());
        assertFalse(whereClause.noMatch());

        whereClause = analyzeSelectWhere("select id, name from parted where (date =1395874800000 or date = 1395961200000) and id = 1");
        assertThat(whereClause.partitions(), containsInAnyOrder(partition1, partition2));
        assertTrue(whereClause.hasQuery());
        assertFalse(whereClause.noMatch());

        whereClause = analyzeSelectWhere("select id, name from parted where date = 1395874800000 and id is null");
        assertEquals(ImmutableList.of(partition1), whereClause.partitions());
        assertTrue(whereClause.hasQuery());
        assertFalse(whereClause.noMatch());

        whereClause = analyzeSelectWhere("select id, name from parted where date is null and id = 1");
        assertEquals(ImmutableList.of(partition3), whereClause.partitions());
        assertTrue(whereClause.hasQuery());
        assertFalse(whereClause.noMatch());

        whereClause = analyzeSelectWhere("select id, name from parted where 1395874700000 < date and date < 1395961200001");
        assertThat(whereClause.partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(whereClause.hasQuery());
        assertFalse(whereClause.noMatch());

        whereClause = analyzeSelectWhere("select id, name from parted where '2014-03-16T22:58:20' < date and date < '2014-03-27T23:00:01'");
        assertThat(whereClause.partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(whereClause.hasQuery());
        assertFalse(whereClause.noMatch());
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