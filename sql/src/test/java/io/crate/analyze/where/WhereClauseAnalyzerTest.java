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
import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.*;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.repositories.RepositorySettingsModule;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.core.collections.Rows;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.operator.any.AnyEqOperator;
import io.crate.operation.operator.any.AnyLikeOperator;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.MockedClusterServiceModule;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class WhereClauseAnalyzerTest extends CrateUnitTest {

    public static final String GENERATED_COL_TABLE_NAME = "generated_col";
    public static final String DOUBLE_GEN_PARTITIONED_TABLE_NAME = "double_gen_parted";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private Analyzer analyzer;
    private AnalysisMetaData ctxMetaData;
    private ThreadPool threadPool;

    private final TransactionContext transactionContext = new TransactionContext();

    @Mock
    private SchemaInfo schemaInfo;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = newMockedThreadPool();
        Injector injector = new ModulesBuilder()
            .add(new MockedClusterServiceModule())
            .add(new PredicateModule())
            .add(new OperatorModule())
            .add(new ScalarFunctionModule())
            .add(new MetaDataSysModule())
            .add(new RepositorySettingsModule())
            .add(new TestMetaDataModule()).createInjector();
        analyzer = injector.getInstance(Analyzer.class);
        ctxMetaData = injector.getInstance(AnalysisMetaData.class);
        Functions functions = injector.getInstance(Functions.class);

        TableInfo genInfo =
            TestingTableInfo.builder(new TableIdent(DocSchemaInfo.NAME, GENERATED_COL_TABLE_NAME), new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
                .add("ts", DataTypes.TIMESTAMP, null)
                .add("x", DataTypes.INTEGER, null)
                .add("y", DataTypes.LONG, null)
                .addGeneratedColumn("day", DataTypes.TIMESTAMP, "date_trunc('day', ts)", true)
                .addGeneratedColumn("minus_y", DataTypes.LONG, "y * -1", true)
                .addGeneratedColumn("x_incr", DataTypes.LONG, "x + 1", false)
                .addPartitions(
                    new PartitionName("generated_col", Arrays.asList(new BytesRef("1420070400000"), new BytesRef("-1"))).asIndexName(),
                    new PartitionName("generated_col", Arrays.asList(new BytesRef("1420156800000"), new BytesRef("-2"))).asIndexName()
                )
                .build(injector.getInstance(Functions.class));
        when(schemaInfo.getTableInfo(GENERATED_COL_TABLE_NAME)).thenReturn(genInfo);

        TableIdent ident = new TableIdent(DocSchemaInfo.NAME, DOUBLE_GEN_PARTITIONED_TABLE_NAME);
        TableInfo doubleGenPartedInfo =
            TestingTableInfo.builder(ident, new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
                .add("x", DataTypes.INTEGER, null)
                .addGeneratedColumn("x1", DataTypes.LONG, "x+1", true)
                .addGeneratedColumn("x2", DataTypes.LONG, "x+2", true)
                .addPartitions(
                    new PartitionName(ident, Arrays.asList(new BytesRef("4"), new BytesRef("5"))).toString(),
                    new PartitionName(ident, Arrays.asList(new BytesRef("5"), new BytesRef("6"))).toString()
                )
                .build(functions);
        when(schemaInfo.getTableInfo(DOUBLE_GEN_PARTITIONED_TABLE_NAME)).thenReturn(doubleGenPartedInfo);
    }

    @After
    public void after() throws Exception {
        threadPool.shutdown();
        threadPool.awaitTermination(1, TimeUnit.SECONDS);
    }

    static final Routing twoNodeRouting = new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
        .put("nodeOne", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put("t1", Arrays.asList(1, 2)).map())
        .put("nodeTow", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put("t1", Arrays.asList(3, 4)).map())
        .map());

    class TestMetaDataModule extends MetaDataModule {
        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            bind(ThreadPool.class).toInstance(threadPool);
            when(schemaInfo.name()).thenReturn(Schemas.DEFAULT_SCHEMA_NAME);
            when(schemaInfo.getTableInfo("users")).thenReturn(
                TestingTableInfo.builder(new TableIdent("doc", "users"), twoNodeRouting)
                    .add("id", DataTypes.STRING, null)
                    .add("name", DataTypes.STRING, null)
                    .add("tags", new ArrayType(DataTypes.STRING), null)
                    .addPrimaryKey("id")
                    .clusteredBy("id")
                    .build());
            when(schemaInfo.getTableInfo("parted")).thenReturn(
                TestingTableInfo.builder(new TableIdent("doc", "parted"), twoNodeRouting)
                    .add("id", DataTypes.INTEGER, null)
                    .add("name", DataTypes.STRING, null)
                    .add("date", DataTypes.TIMESTAMP, null, true)
                    .add("obj", DataTypes.OBJECT, null, ColumnPolicy.IGNORED)
                    .addPartitions(
                        new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).asIndexName(),
                        new PartitionName("parted", Arrays.asList(new BytesRef("1395961200000"))).asIndexName(),
                        new PartitionName("parted", new ArrayList<BytesRef>() {{
                            add(null);
                        }}).asIndexName())
                    .build());
            when(schemaInfo.getTableInfo("parted_pk")).thenReturn(
                TestingTableInfo.builder(new TableIdent("doc", "parted"), twoNodeRouting)
                    .addPrimaryKey("id").addPrimaryKey("date")
                    .add("id", DataTypes.INTEGER, null)
                    .add("name", DataTypes.STRING, null)
                    .add("date", DataTypes.TIMESTAMP, null, true)
                    .add("obj", DataTypes.OBJECT, null, ColumnPolicy.IGNORED)
                    .addPartitions(
                        new PartitionName("parted_pk", Arrays.asList(new BytesRef("1395874800000"))).asIndexName(),
                        new PartitionName("parted_pk", Arrays.asList(new BytesRef("1395961200000"))).asIndexName(),
                        new PartitionName("parted_pk", new ArrayList<BytesRef>() {{
                            add(null);
                        }}).asIndexName())
                    .build());
            when(schemaInfo.getTableInfo("bystring")).thenReturn(
                TestingTableInfo.builder(new TableIdent("doc", "bystring"), twoNodeRouting)
                    .add("name", DataTypes.STRING, null)
                    .add("score", DataTypes.DOUBLE, null)
                    .addPrimaryKey("name")
                    .clusteredBy("name")
                    .build());
            when(schemaInfo.getTableInfo("users_multi_pk")).thenReturn(
                TestingTableInfo.builder(new TableIdent("doc", "users_multi_pk"), twoNodeRouting)
                    .add("id", DataTypes.LONG, null)
                    .add("name", DataTypes.STRING, null)
                    .add("details", DataTypes.OBJECT, null)
                    .add("awesome", DataTypes.BOOLEAN, null)
                    .add("friends", new ArrayType(DataTypes.OBJECT), null, ColumnPolicy.DYNAMIC)
                    .addPrimaryKey("id")
                    .addPrimaryKey("name")
                    .clusteredBy("id")
                    .build());
            when(schemaInfo.getTableInfo("pk4")).thenReturn(
                TestingTableInfo.builder(new TableIdent("doc", "pk4"), twoNodeRouting)
                    .add("i1", DataTypes.INTEGER, null)
                    .add("i2", DataTypes.INTEGER, null)
                    .add("i3", DataTypes.INTEGER, null)
                    .add("i4", DataTypes.INTEGER, null)
                    .addPrimaryKey("i1")
                    .addPrimaryKey("i2")
                    .addPrimaryKey("i3")
                    .addPrimaryKey("i4")
                    .build());
            when(schemaInfo.getTableInfo("users_clustered_by_only")).thenReturn(
                TestingTableInfo.builder(new TableIdent("doc", "users_clustered_by_only"), twoNodeRouting)
                    .add("id", DataTypes.LONG, null)
                    .add("name", DataTypes.STRING, null)
                    .add("details", DataTypes.OBJECT, null)
                    .add("awesome", DataTypes.BOOLEAN, null)
                    .add("friends", new ArrayType(DataTypes.OBJECT), null, ColumnPolicy.DYNAMIC)
                    .clusteredBy("id")
                    .build());
            schemaBinder.addBinding(Schemas.DEFAULT_SCHEMA_NAME).toInstance(schemaInfo);
        }
    }

    private DeleteAnalyzedStatement analyzeDelete(String stmt, Object[][] bulkArgs) {
        return (DeleteAnalyzedStatement) analyzer.boundAnalyze(SqlParser.createStatement(stmt),
            SessionContext.SYSTEM_SESSION,
            new ParameterContext(Row.EMPTY, Rows.of(bulkArgs))).analyzedStatement();
    }

    private DeleteAnalyzedStatement analyzeDelete(String stmt) {
        return analyzeDelete(stmt, new Object[0][]);
    }

    private UpdateAnalyzedStatement analyzeUpdate(String stmt) {
        return (UpdateAnalyzedStatement) analyzer.boundAnalyze(SqlParser.createStatement(stmt),
            SessionContext.SYSTEM_SESSION,
            new ParameterContext(Row.EMPTY, Collections.<Row>emptyList())).analyzedStatement();
    }

    private WhereClause analyzeSelect(String stmt, Object... args) {
        SelectAnalyzedStatement statement = (SelectAnalyzedStatement) analyzer.boundAnalyze(SqlParser.createStatement(stmt),
            SessionContext.SYSTEM_SESSION,
            new ParameterContext(new RowN(args), Collections.<Row>emptyList())).analyzedStatement();
        return statement.relation().querySpec().where();
    }

    private WhereClause analyzeSelectWhere(String stmt) {
        return analyzeSelect(stmt);
    }

    @Test
    public void testWhereSinglePKColumnEq() throws Exception {
        DeleteAnalyzedStatement statement = analyzeDelete("delete from users where id = ?", new Object[][]{
            new Object[]{1},
            new Object[]{2},
            new Object[]{3},
        });
        DocTableRelation tableRelation = statement.analyzedRelation();
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(
            ctxMetaData.functions(), ctxMetaData.referenceResolver(), tableRelation);
        assertThat(whereClauseAnalyzer.analyze(statement.whereClauses().get(0), transactionContext).docKeys().get(), contains(isDocKey("1")));
        assertThat(whereClauseAnalyzer.analyze(statement.whereClauses().get(1), transactionContext).docKeys().get(), contains(isDocKey("2")));
        assertThat(whereClauseAnalyzer.analyze(statement.whereClauses().get(2), transactionContext).docKeys().get(), contains(isDocKey("3")));
    }

    @Test
    public void testSelectByIdWithCustomRouting() throws Exception {
        WhereClause whereClause = analyzeSelect("select name from users_clustered_by_only where _id=1");
        assertFalse(whereClause.docKeys().isPresent());
    }

    @Test
    public void testSelectByIdWithPartitions() throws Exception {
        WhereClause whereClause = analyzeSelect("select id from parted where _id=1");
        assertFalse(whereClause.docKeys().isPresent());
    }

    @Test
    public void testSelectWherePartitionedByColumn() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select id from parted where date = 1395874800000");
        assertThat(whereClause.hasQuery(), is(false));
        assertThat(whereClause.noMatch(), is(false));
        assertThat(whereClause.partitions(),
            Matchers.contains(new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).asIndexName()));
    }

    @Test
    public void testSelectPartitionedByPK() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select id from parted_pk where id = 1 and date = 1395874800000");
        assertThat(whereClause.docKeys().get(), contains(isDocKey(1, 1395874800000L)));
        // not partitions if docKeys are there
        assertThat(whereClause.partitions(), empty());
    }

    @Test
    public void testSelectFromPartitionedTableWithoutPKInWhereClause() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select id from parted where match(name, 'name')");
        assertThat(whereClause.docKeys().isPresent(), is(false));
        assertThat(whereClause.partitions(), empty());
    }

    @Test
    public void testWherePartitionedByColumn() throws Exception {
        DeleteAnalyzedStatement statement = analyzeDelete("delete from parted where date = 1395874800000");
        WhereClause whereClause = statement.whereClauses().get(0);

        assertThat(whereClause.hasQuery(), is(false));
        assertThat(whereClause.noMatch(), is(false));
        assertThat(whereClause.partitions(),
            Matchers.contains(new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).asIndexName()));
    }

    @Test
    public void testUpdateWherePartitionedByColumn() throws Exception {
        UpdateAnalyzedStatement updateAnalyzedStatement = analyzeUpdate("update parted set id = 2 where date = 1395874800000");
        UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalyzedStatement = updateAnalyzedStatement.nestedStatements().get(0);

        assertThat(nestedAnalyzedStatement.whereClause().hasQuery(), is(false));
        assertThat(nestedAnalyzedStatement.whereClause().noMatch(), is(false));

        assertEquals(ImmutableList.of(
            new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).asIndexName()),
            nestedAnalyzedStatement.whereClause().partitions()
        );
    }

    @Test
    public void testClusteredByValueContainsComma() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select * from bystring where name = 'a,b,c'");
        assertThat(whereClause.clusteredBy().get(), contains(isLiteral("a,b,c")));
        assertThat(whereClause.docKeys().get().size(), is(1));
        assertThat(whereClause.docKeys().get().getOnlyKey(), isDocKey("a,b,c"));
    }

    @Test
    public void testEmptyClusteredByValue() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select * from bystring where name = ''");
        assertThat(whereClause.clusteredBy().get(), contains(isLiteral("")));
        assertThat(whereClause.docKeys().get().getOnlyKey(), isDocKey(""));
    }

    @Test
    public void testClusteredBy() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select name from users where id=1");
        assertThat(whereClause.clusteredBy().get(), contains(isLiteral("1")));
        assertThat(whereClause.docKeys().get().getOnlyKey(), isDocKey("1"));

        whereClause = analyzeSelectWhere("select name from users where id=1 or id=2");

        assertThat(whereClause.docKeys().get().size(), is(2));
        assertThat(whereClause.docKeys().get(), containsInAnyOrder(isDocKey("1"), isDocKey("2")));

        assertThat(whereClause.clusteredBy().get(), containsInAnyOrder(isLiteral("1"), isLiteral("2")));
    }


    @Test
    public void testClusteredByOnly() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select name from users_clustered_by_only where id=1");
        assertFalse(whereClause.docKeys().isPresent());
        assertThat(whereClause.clusteredBy().get(), contains(isLiteral(1L)));

        whereClause = analyzeSelectWhere("select name from users_clustered_by_only where id=1 or id=2");
        assertFalse(whereClause.docKeys().isPresent());
        assertThat(whereClause.clusteredBy().get(), containsInAnyOrder(isLiteral(1L), isLiteral(2L)));

        whereClause = analyzeSelectWhere("select name from users_clustered_by_only where id in (3,4,5)");
        assertFalse(whereClause.docKeys().isPresent());
        assertThat(whereClause.clusteredBy().get(), containsInAnyOrder(
            isLiteral(3L), isLiteral(4L), isLiteral(5L)));


        // TODO: optimize this case: there are two routing values here, which are currently not set
        whereClause = analyzeSelectWhere("select name from users_clustered_by_only where id=1 and id=2");
        assertFalse(whereClause.docKeys().isPresent());
        assertFalse(whereClause.clusteredBy().isPresent());
    }

    @Test
    public void testCompositePrimaryKey() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select name from users_multi_pk where id=1");
        assertFalse(whereClause.docKeys().isPresent());
        assertThat(whereClause.clusteredBy().get(), contains(isLiteral(1L)));

        whereClause = analyzeSelectWhere("select name from users_multi_pk where id=1 and name='Douglas'");
        assertThat(whereClause.docKeys().get(), contains(isDocKey(1L, "Douglas")));
        assertThat(whereClause.clusteredBy().get(), contains(isLiteral(1L)));

        whereClause = analyzeSelectWhere("select name from users_multi_pk where id=1 or id=2 and name='Douglas'");
        assertFalse(whereClause.docKeys().isPresent());
        assertThat(whereClause.clusteredBy().get(), containsInAnyOrder(
            isLiteral(1L), isLiteral(2L)));

        whereClause = analyzeSelectWhere("select name from users_multi_pk where id=1 and name='Douglas' or name='Arthur'");
        assertFalse(whereClause.docKeys().isPresent());
        assertFalse(whereClause.clusteredBy().isPresent());
    }

    @Test
    public void testPrimaryKeyAndVersion() throws Exception {
        WhereClause whereClause = analyzeSelectWhere(
            "select name from users where id = 2 and \"_version\" = 1");
        assertThat(whereClause.docKeys().get().getOnlyKey(), isDocKey("2", 1L));
    }

    @Test
    public void testMultiplePrimaryKeys() throws Exception {
        WhereClause whereClause = analyzeSelectWhere(
            "select name from users where id = 2 or id = 1");
        assertThat(whereClause.docKeys().get(), containsInAnyOrder(isDocKey("1"), isDocKey("2")));
        assertThat(whereClause.clusteredBy().get(), containsInAnyOrder(isLiteral("1"), isLiteral("2")));
    }

    @Test
    public void testMultiplePrimaryKeysAndInvalidColumn() throws Exception {
        WhereClause whereClause = analyzeSelectWhere(
            "select name from users where id = 2 or id = 1 and name = 'foo'");
        assertFalse(whereClause.docKeys().isPresent());
    }

    @Test
    public void testNotEqualsDoesntMatchPrimaryKey() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select name from users where id != 1");
        assertFalse(whereClause.docKeys().isPresent());
        assertFalse(whereClause.clusteredBy().isPresent());
    }

    @Test
    public void testMultipleCompoundPrimaryKeys() throws Exception {
        WhereClause whereClause = analyzeSelectWhere(
            "select * from pk4 where (i1=1 and i2=2 and i3=3 and i4=4) " +
            "or (i1=1 and i2=5 and i3=6 and i4=4)");

        assertThat(whereClause.docKeys().get(), containsInAnyOrder(
            isDocKey(1, 2, 3, 4), isDocKey(1, 5, 6, 4)
        ));
        assertFalse(whereClause.clusteredBy().isPresent());

        whereClause = analyzeSelectWhere(
            "select * from pk4 where (i1=1 and i2=2 and i3=3 and i4=4) " +
            "or (i1=1 and i2=5 and i3=6 and i4=4) or i1 = 3");
        assertFalse(whereClause.docKeys().isPresent());
        assertFalse(whereClause.clusteredBy().isPresent());
    }

    @Test
    public void test1ColPrimaryKey() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select name from users where id='jalla'");

        assertThat(whereClause.docKeys().get(), contains(isDocKey("jalla")));

        whereClause = analyzeSelectWhere("select name from users where 'jalla'=id");
        assertThat(whereClause.docKeys().get(), contains(isDocKey("jalla")));

        whereClause = analyzeSelectWhere("select name from users where id='jalla' and id='jalla'");
        assertThat(whereClause.docKeys().get(), contains(isDocKey("jalla")));

        whereClause = analyzeSelectWhere("select name from users where id='jalla' and (id='jalla' or 1=1)");
        assertThat(whereClause.docKeys().get(), contains(isDocKey("jalla")));

        // since the id is unique it is not possible to have a result here, this is not optimized and just results in
        // no found primary keys
        whereClause = analyzeSelectWhere("select name from users where id='jalla' and id='kelle'");
        assertFalse(whereClause.docKeys().isPresent());


        whereClause = analyzeSelectWhere("select name from users where id='jalla' or name = 'something'");
        assertFalse(whereClause.docKeys().isPresent());
        assertFalse(whereClause.noMatch());

        whereClause = analyzeSelectWhere("select name from users where name = 'something'");
        assertFalse(whereClause.docKeys().isPresent());
        assertFalse(whereClause.noMatch());

    }

    @Test
    public void test4ColPrimaryKey() throws Exception {
        WhereClause whereClause = analyzeSelectWhere(
            "select * from pk4 where i1=10 and i2=20 and i3=30 and i4=40");
        assertThat(whereClause.docKeys().get(), contains(isDocKey(10, 20, 30, 40)));
        assertFalse(whereClause.noMatch());

        whereClause = analyzeSelectWhere(
            "select * from pk4 where i1=10 and i2=20 and i3=30 and i4=40 and i1=10");
        assertThat(whereClause.docKeys().get(), contains(isDocKey(10, 20, 30, 40)));
        assertFalse(whereClause.noMatch());


        whereClause = analyzeSelectWhere("select * from pk4 where i1=1");
        assertFalse(whereClause.docKeys().isPresent());
        assertFalse(whereClause.noMatch());

        whereClause = analyzeSelectWhere(
            "select * from pk4 where i1=10 and i2=20 and i3=30 and i4=40 and i1=11");
        assertFalse(whereClause.docKeys().isPresent());
    }

    @Test
    public void test1ColPrimaryKeySetLiteralDiffMatches() throws Exception {
        WhereClause whereClause = analyzeSelectWhere(
            "select name from users where id in ('jalla', 'kelle') and id in ('jalla', 'something')");
        assertFalse(whereClause.noMatch());
        assertThat(whereClause.docKeys().get(), contains(isDocKey("jalla")));
    }

    @Test
    public void test1ColPrimaryKeySetLiteral() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select name from users where id in ('1', '2')");
        assertFalse(whereClause.noMatch());
        assertThat(whereClause.docKeys().get(), containsInAnyOrder(isDocKey("1"), isDocKey("2")));
    }

    @Test
    public void test1ColPrimaryKeyNotSetLiteral() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select name from users where id not in ('jalla', 'kelle')");
        assertFalse(whereClause.noMatch());
        assertFalse(whereClause.docKeys().isPresent());
        assertFalse(whereClause.clusteredBy().isPresent());
    }

    @Test
    public void test4ColPrimaryKeySetLiteral() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select * from pk4 where i1=10 and i2=20 and" +
                                                     " i3 in (30, 31) and i4=40");
        assertThat(whereClause.docKeys().get(), containsInAnyOrder(
            isDocKey(10, 20, 30, 40), isDocKey(10, 20, 31, 40)));
    }

    @Test
    public void test4ColPrimaryKeyWithOr() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select * from pk4 where i1=10 and i2=20 and " +
                                                     "(i3=30 or i3=31) and i4=40");
        assertEquals(2, whereClause.docKeys().get().size());
        assertThat(whereClause.docKeys().get(), containsInAnyOrder(
            isDocKey(10, 20, 30, 40), isDocKey(10, 20, 31, 40)));
    }

    @Test
    public void testSelectFromPartitionedTable() throws Exception {
        String partition1 = new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).asIndexName();
        String partition2 = new PartitionName("parted", Arrays.asList(new BytesRef("1395961200000"))).asIndexName();
        String partition3 = new PartitionName("parted", new ArrayList<BytesRef>() {{
            add(null);
        }}).asIndexName();

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
         * obj['col'] = 'undefined' => null as col doesn't exist
         *
         *  partition1: not (true  and null) -> not (null)  -> null -> no match
         *  partition2: not (false and null) -> not (false) -> true -> match
         *  partition3: not (null  and null) -> not (null)  -> null -> no match
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

    @Test
    public void testAnyInvalidArrayType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot cast ['foo', 'bar', 'baz'] to type boolean_array");
        analyzeSelectWhere("select * from users_multi_pk where awesome = any(['foo', 'bar', 'baz'])");
    }

    @Test
    public void testInConvertedToAnyIfOnlyLiterals() throws Exception {
        StringBuilder sb = new StringBuilder("select id from sys.shards where id in (");
        int i = 0;
        for (; i < 1500; i++) {
            sb.append(i);
            sb.append(',');
        }
        sb.append(i++);
        sb.append(')');
        String s = sb.toString();

        WhereClause whereClause = analyzeSelectWhere(s);
        assertThat(whereClause.query(), isFunction(AnyEqOperator.NAME, ImmutableList.<DataType>of(DataTypes.INTEGER, new SetType(DataTypes.INTEGER))));
    }

    @Test
    public void testInNormalizedToAnyWithScalars() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select * from users where id in (null, 1+2, 3+4, abs(-99))");
        assertThat(whereClause.query(), isFunction(AnyEqOperator.NAME));
        assertThat(whereClause.docKeys().isPresent(), is(true));
        assertThat(whereClause.docKeys().get(), containsInAnyOrder(isNullDocKey(), isDocKey("3"), isDocKey("7"), isDocKey("99")));
    }

    @Test
    public void testAnyEqConvertableArrayTypeLiterals() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select * from users where name = any([1, 2, 3])");
        assertThat(whereClause.query(), isFunction(AnyEqOperator.NAME, ImmutableList.<DataType>of(DataTypes.STRING, new ArrayType(DataTypes.STRING))));
    }

    @Test
    public void testAnyLikeConvertableArrayTypeLiterals() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select * from users where name like any([1, 2, 3])");
        assertThat(whereClause.query(), isFunction(AnyLikeOperator.NAME, ImmutableList.<DataType>of(DataTypes.STRING, new ArrayType(DataTypes.STRING))));
    }

    @Test
    public void testAnyLikeArrayLiteral() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select * from users where name like any(['a', 'b', 'c'])");
        assertThat(whereClause.query(), isFunction(AnyLikeOperator.NAME, ImmutableList.<DataType>of(DataTypes.STRING, new ArrayType(DataTypes.STRING))));
    }

    @Test
    public void testEqualGenColOptimization() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select * from generated_col where y = 1");
        assertThat(whereClause.partitions().size(), is(1));
        assertThat(whereClause.partitions().get(0), is(new PartitionName("generated_col", Arrays.asList(new BytesRef("1420070400000"), new BytesRef("-1"))).asIndexName()));
    }

    @Test
    public void testNonPartitionedNotOptimized() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select * from generated_col where x = 1");
        assertThat(whereClause.query(), isSQL("(doc.generated_col.x = 1)"));
    }

    @Test
    public void testGtGenColOptimization() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select * from generated_col where ts > '2015-01-02T12:00:00'");
        assertThat(whereClause.partitions().size(), is(1));
        assertThat(whereClause.partitions().get(0), is(new PartitionName("generated_col", Arrays.asList(new BytesRef("1420156800000"), new BytesRef("-2"))).asIndexName()));
    }

    @Test
    public void testGenColRoundingFunctionNoSwappingOperatorOptimization() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select * from generated_col where ts >= '2015-01-02T12:00:00'");
        assertThat(whereClause.partitions().size(), is(1));
        assertThat(whereClause.partitions().get(0), is(new PartitionName("generated_col", Arrays.asList(new BytesRef("1420156800000"), new BytesRef("-2"))).asIndexName()));
    }

    @Test
    public void testMultiplicationGenColNoOptimization() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select * from generated_col where y > 1");
        // no optimization is done
        assertThat(whereClause.partitions().size(), is(0));
        assertThat(whereClause.noMatch(), is(false));
    }

    @Test
    public void testMultipleColumnsOptimization() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select * from generated_col where ts > '2015-01-01T12:00:00' and y = 1");
        assertThat(whereClause.partitions().size(), is(1));
        assertThat(whereClause.partitions().get(0), is(new PartitionName("generated_col", Arrays.asList(new BytesRef("1420070400000"), new BytesRef("-1"))).asIndexName()));
    }

    @Test
    public void testColumnReferencedTwiceInGeneratedColumnPartitioned() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select * from double_gen_parted where x = 4");
        assertThat(whereClause.query(), isSQL("(doc.double_gen_parted.x = 4)"));
        assertThat(whereClause.partitions().size(), is(1));
        assertThat(whereClause.partitions().get(0), is(".partitioned.double_gen_parted.0813a0hm"));
    }

    @Test
    public void testOptimizationNonRoundingFunctionGreater() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select * from double_gen_parted where x > 3");
        assertThat(whereClause.query(), isSQL("(doc.double_gen_parted.x > 3)"));
        assertThat(whereClause.partitions().size(), is(1));
        assertThat(whereClause.partitions().get(0), is(".partitioned.double_gen_parted.0813a0hm"));
    }

    @Test
    public void testGenColRangeOptimization() throws Exception {
        WhereClause whereClause = analyzeSelectWhere("select * from generated_col where ts >= '2015-01-01T12:00:00' and ts <= '2015-01-02T00:00:00'");
        assertThat(whereClause.partitions().size(), is(2));
        assertThat(whereClause.partitions().get(0), is(new PartitionName("generated_col", Arrays.asList(new BytesRef("1420070400000"), new BytesRef("-1"))).asIndexName()));
        assertThat(whereClause.partitions().get(1), is(new PartitionName("generated_col", Arrays.asList(new BytesRef("1420156800000"), new BytesRef("-2"))).asIndexName()));
    }
}
