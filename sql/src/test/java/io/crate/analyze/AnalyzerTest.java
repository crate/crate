/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.ReferenceInfo;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operator.Input;
import io.crate.operator.aggregation.impl.AggregationImplModule;
import io.crate.operator.aggregation.impl.AverageAggregation;
import io.crate.operator.aggregation.impl.CollectSetAggregation;
import io.crate.operator.operator.*;
import io.crate.operator.reference.sys.cluster.SysClusterExpression;
import io.crate.operator.reference.sys.node.NodeLoadExpression;
import io.crate.operator.scalar.CollectionCountFunction;
import io.crate.operator.scalar.ScalarFunctionModule;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.cratedb.sql.AmbiguousAliasException;
import org.cratedb.sql.CrateException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.os.OsStats;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AnalyzerTest {

    private static final TableIdent TEST_DOC_TABLE_IDENT = new TableIdent(null, "users");
    private static final FunctionInfo ABS_FUNCTION_INFO = new FunctionInfo(
            new FunctionIdent("abs", Arrays.asList(DataType.LONG)),
            DataType.LONG);
    private Injector injector;
    private Analyzer analyzer;

    private static final ReferenceInfo LOAD_INFO = SysNodesTableInfo.INFOS.get(new ColumnIdent("load"));
    private static final ReferenceInfo LOAD1_INFO = SysNodesTableInfo.INFOS.get(new ColumnIdent("load", "1"));
    private static final ReferenceInfo LOAD5_INFO = SysNodesTableInfo.INFOS.get(new ColumnIdent("load", "5"));

    private static final ReferenceInfo CLUSTER_NAME_INFO = SysClusterTableInfo.INFOS.get(new ColumnIdent("name"));

    class ClusterNameExpression extends SysClusterExpression<BytesRef> {

        protected ClusterNameExpression() {
            super(CLUSTER_NAME_INFO.ident().columnIdent().name());
        }

        @Override
        public BytesRef value() {
            return new BytesRef("testcluster");
        }
    }

    Routing shardRouting = new Routing(ImmutableMap.<String, Map<String, Set<Integer>>>builder()
            .put("nodeOne", ImmutableMap.<String, Set<Integer>>of("t1", ImmutableSet.of(1, 2)))
            .put("nodeTow", ImmutableMap.<String, Set<Integer>>of("t1", ImmutableSet.of(3, 4)))
            .build());

    static class AbsFunction implements Scalar<Long> {

        @Override
        public Long evaluate(Input<?>... args) {
            if (args == null || args.length == 0) {
                return 0l;
            }
            return Math.abs(((Number) args[0].value()).longValue());
        }

        @Override
        public FunctionInfo info() {
            return ABS_FUNCTION_INFO;
        }


        @Override
        public Symbol normalizeSymbol(Function symbol) {
            return symbol;
        }
    }

    class TestMetaDataModule extends MetaDataModule {

        @Override
        protected void bindReferences() {
            super.bindReferences();
            referenceBinder.addBinding(LOAD_INFO.ident()).to(NodeLoadExpression.class).asEagerSingleton();
            referenceBinder.addBinding(CLUSTER_NAME_INFO.ident()).toInstance(new ClusterNameExpression());
        }

        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            TableIdent userTableIdent = TEST_DOC_TABLE_IDENT;
            TableInfo userTableInfo = TestingTableInfo.builder(userTableIdent, RowGranularity.DOC, shardRouting)
                    .add("id", DataType.LONG, null)
                    .add("name", DataType.STRING, null)
                    .add("details", DataType.OBJECT, null)
                    .add("awesome", DataType.BOOLEAN, null)
                    .build();
            when(schemaInfo.getTableInfo(userTableIdent.name())).thenReturn(userTableInfo);
            schemaBinder.addBinding(DocSchemaInfo.NAME).toInstance(schemaInfo);
        }

        @Override
        protected void bindFunctions() {
            super.bindFunctions();
            functionBinder.addBinding(ABS_FUNCTION_INFO.ident()).to(AbsFunction.class);
        }
    }

    /**
     * borrowed from {@link io.crate.operator.reference.sys.TestGlobalSysExpressions}
     * // TODO share it
     */
    class TestModule extends AbstractModule {

        @Override
        protected void configure() {
            // clusterService.state().metaData().settings()
            ClusterService clusterService = mock(ClusterService.class);
            ClusterState state = mock(ClusterState.class);
            MetaData metaData = mock(MetaData.class);
            when(metaData.settings()).thenReturn(ImmutableSettings.EMPTY);
            when(state.metaData()).thenReturn(metaData);
            when(clusterService.state()).thenReturn(state);
            bind(ClusterService.class).toInstance(clusterService);
            bind(Settings.class).toInstance(ImmutableSettings.EMPTY);
            OsService osService = mock(OsService.class);
            OsStats osStats = mock(OsStats.class);
            when(osService.stats()).thenReturn(osStats);
            when(osStats.loadAverage()).thenReturn(new double[]{1, 5, 15});
            bind(OsService.class).toInstance(osService);
            Discovery discovery = mock(Discovery.class);
            bind(Discovery.class).toInstance(discovery);
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(discovery.localNode()).thenReturn(node);
            when(node.getId()).thenReturn("node-id-1");
            when(node.getName()).thenReturn("node 1");
        }
    }

    private Analysis analyze(String statement) {
        return analyzer.analyze(SqlParser.createStatement(statement));
    }

    private Analysis analyze(String statement, Object[] params) {
        return analyzer.analyze(SqlParser.createStatement(statement), params);
    }

    @Before
    public void setUp() throws Exception {
        injector = new ModulesBuilder()
                .add(new TestModule())
                .add(new TestMetaDataModule())
                .add(new MetaDataSysModule())
                .add(new AggregationImplModule())
                .add(new OperatorModule())
                .add(new ScalarFunctionModule())
                .createInjector();
        analyzer = injector.getInstance(Analyzer.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGroupedSelectMissingOutput() throws Exception {
        Statement statement = SqlParser.createStatement("select load['5'] from sys.nodes group by load['1']");
        analyzer.analyze(statement);
    }


    @Test
    public void testOrderedSelect() throws Exception {
        Statement statement = SqlParser.createStatement("select load['1'] from sys.nodes order by load['5'] desc");
        SelectAnalysis analysis = (SelectAnalysis) analyzer.analyze(statement);
        assertEquals(analysis.table().ident(), SysNodesTableInfo.IDENT);
        assertNull(analysis.limit());

        assertFalse(analysis.hasGroupBy());
        assertTrue(analysis.isSorted());
        assertThat(analysis.rowGranularity(), is(RowGranularity.NODE));

        assertEquals(1, analysis.outputSymbols().size());
        assertEquals(1, analysis.sortSymbols().size());
        assertEquals(1, analysis.reverseFlags().length);

        assertEquals(LOAD5_INFO, ((Reference) analysis.sortSymbols().get(0)).info());
    }

    @Test
    public void testGroupKeyNotInResultColumnList() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis) analyze("select count(*) from sys.nodes group by name");

        assertThat(analysis.groupBy().size(), is(1));
        assertThat(analysis.outputNames().get(0), is("count(*)"));
    }

    @Test
    public void testGroupByOnAlias() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis) analyze("select count(*), name as n from sys.nodes group by n");
        assertThat(analysis.groupBy().size(), is(1));
        assertThat(analysis.outputNames().get(0), is("count(*)"));
        assertThat(analysis.outputNames().get(1), is("n"));

        assertEquals(analysis.groupBy().get(0), analysis.outputSymbols().get(1));
    }

    @Test
    public void testGroupByOnOrdinal() throws Exception {
        // just like in postgres access by ordinal starts with 1
        SelectAnalysis analysis = (SelectAnalysis) analyze("select count(*), name as n from sys.nodes group by 2");
        assertThat(analysis.groupBy().size(), is(1));
        assertEquals(analysis.groupBy().get(0), analysis.outputSymbols().get(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGroupByOnInvalidOrdinal() throws Exception {
        analyze("select count(*), name from sys.nodes group by -4");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGroupByOnOrdinalAggregation() throws Exception {
        analyze("select count(*), name as n from sys.nodes group by 1");
    }

    @Test
    public void testNegativeLiteral() throws Exception {
        Analysis analyze = analyze("select * from sys.nodes where port['http'] = -400");
        Function whereClause = analyze.whereClause();
        Symbol symbol = whereClause.arguments().get(1);
        assertThat(((IntegerLiteral) symbol).value(), is(-400));
    }

    @Test
    public void testGroupedSelect() throws Exception {
        Statement statement = SqlParser.createStatement("select load['1'], count(*) from sys.nodes group by load['1']");
        SelectAnalysis analysis = (SelectAnalysis) analyzer.analyze(statement);
        assertEquals(analysis.table().ident(), SysNodesTableInfo.IDENT);
        assertNull(analysis.limit());

        assertThat(analysis.rowGranularity(), is(RowGranularity.NODE));
        assertTrue(analysis.hasGroupBy());
        assertEquals(2, analysis.outputSymbols().size());
        assertEquals(1, analysis.groupBy().size());
        assertEquals(LOAD1_INFO, ((Reference) analysis.groupBy().get(0)).info());

    }


    @Test
    public void testSimpleSelect() throws Exception {
        Statement statement = SqlParser.createStatement("select load['5'] from sys.nodes limit 2");
        SelectAnalysis analysis = (SelectAnalysis) analyzer.analyze(statement);
        assertEquals(analysis.table().ident(), SysNodesTableInfo.IDENT);
        assertEquals(new Integer(2), analysis.limit());

        assertFalse(analysis.hasGroupBy());

        assertThat(analysis.rowGranularity(), is(RowGranularity.NODE));

        assertEquals(SysNodesTableInfo.IDENT, analysis.table().ident());
        assertEquals(1, analysis.outputSymbols().size());
        Reference col1 = (Reference) analysis.outputSymbols().get(0);
        assertEquals(LOAD5_INFO, col1.info());

    }

    @Test
    public void testAggregationSelect() throws Exception {
        Statement statement = SqlParser.createStatement("select avg(load['5']) from sys.nodes");
        SelectAnalysis analysis = (SelectAnalysis) analyzer.analyze(statement);
        assertEquals(SysNodesTableInfo.IDENT, analysis.table().ident());

        assertThat(analysis.rowGranularity(), is(RowGranularity.NODE));

        assertFalse(analysis.hasGroupBy());
        assertEquals(1, analysis.outputSymbols().size());
        Function col1 = (Function) analysis.outputSymbols().get(0);
        assertTrue(col1.info().isAggregate());
        assertEquals(AverageAggregation.NAME, col1.info().ident().name());
    }

    @Test
    public void testAllColumnCluster() throws Exception {
        Analysis analyze = analyze("select * from sys.cluster");
        assertThat(analyze.outputNames().size(), is(2));
        assertThat(analyze.outputNames().get(0), is("id"));
        assertThat(analyze.outputNames().get(1), is("name"));

        assertThat(analyze.outputSymbols().size(), is(2));
    }

    @Test
    public void testAllColumnNodes() throws Exception {
        Analysis analyze = analyze("select id, * from sys.nodes");
        assertThat(analyze.outputNames().get(0), is("id"));
        assertThat(analyze.outputNames().get(1), is("id"));
        assertThat(analyze.outputNames().size(), is(8));
        assertEquals(analyze.outputNames().size(), analyze.outputSymbols().size());
    }

    @Test
    public void testWhereSelect() throws Exception {
        Statement statement = SqlParser.createStatement("select load from sys.nodes " +
                "where load['1'] = 1.2 or 1 >= load['5']");
        SelectAnalysis analysis = (SelectAnalysis) analyzer.analyze(statement);
        assertEquals(SysNodesTableInfo.IDENT, analysis.table().ident());

        assertThat(analysis.rowGranularity(), is(RowGranularity.NODE));

        assertFalse(analysis.hasGroupBy());

        Function whereClause = analysis.whereClause();
        assertEquals(OrOperator.NAME, whereClause.info().ident().name());
        assertFalse(whereClause.info().isAggregate());

        Function left = (Function) whereClause.arguments().get(0);
        assertEquals(EqOperator.NAME, left.info().ident().name());
        assertThat(left.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(left.arguments().get(1), IsInstanceOf.instanceOf(DoubleLiteral.class));

        Function right = (Function) whereClause.arguments().get(1);
        assertEquals(LteOperator.NAME, right.info().ident().name());
        assertThat(right.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(right.arguments().get(1), IsInstanceOf.instanceOf(DoubleLiteral.class));
    }

    @Test
    public void testSelectWithParameters() throws Exception {
        Statement statement = SqlParser.createStatement("select load from sys.nodes " +
                "where load['1'] = ? or load['5'] <= ? or load['15'] >= ? or load['1'] = ? " +
                "or load['1'] = ? or name = ?");
        Analysis analysis = analyzer.analyze(statement, new Object[]{
                1.2d,
                2.4f,
                2L,
                3,
                new Short("1"),
                "node 1"
        });
        Function whereClause = analysis.whereClause();
        assertEquals(OrOperator.NAME, whereClause.info().ident().name());
        assertFalse(whereClause.info().isAggregate());

        Function function = (Function) whereClause.arguments().get(0);
        assertEquals(OrOperator.NAME, function.info().ident().name());
        function = (Function) function.arguments().get(1);
        assertEquals(EqOperator.NAME, function.info().ident().name());
        assertThat(function.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(function.arguments().get(1), IsInstanceOf.instanceOf(DoubleLiteral.class));

        function = (Function) whereClause.arguments().get(1);
        assertEquals(EqOperator.NAME, function.info().ident().name());
        assertThat(function.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(function.arguments().get(1), IsInstanceOf.instanceOf(StringLiteral.class));
    }

    @Test
    public void testOutputNames() throws Exception {
        Analysis analyze = analyze("select load as l, id, load['1'] from sys.nodes");
        assertThat(analyze.outputNames().size(), is(3));
        assertThat(analyze.outputNames().get(0), is("l"));
        assertThat(analyze.outputNames().get(1), is("id"));
        assertThat(analyze.outputNames().get(2), is("load['1']"));
    }

    @Test
    public void testDuplicateOutputNames() throws Exception {
        Analysis analyze = analyze("select load as l, load['1'] as l from sys.nodes");
        assertThat(analyze.outputNames().size(), is(2));
        assertThat(analyze.outputNames().get(0), is("l"));
        assertThat(analyze.outputNames().get(1), is("l"));
    }

    @Test
    public void testOrderByOnAlias() throws Exception {
        SelectAnalysis analyze = (SelectAnalysis) analyze("select load as l from sys.nodes order by l");
        assertThat(analyze.outputNames().size(), is(1));
        assertThat(analyze.outputNames().get(0), is("l"));

        assertTrue(analyze.isSorted());
        assertThat(analyze.sortSymbols().size(), is(1));
        assertThat(analyze.sortSymbols().get(0), is(analyze.outputSymbols().get(0)));
    }

    @Test(expected = AmbiguousAliasException.class)
    public void testAmbiguousOrderByOnAlias() throws Exception {
        analyze("select id as load, load from sys.nodes order by load");
    }

    @Test
    public void testOffsetSupportInAnalyzer() throws Exception {
        SelectAnalysis analyze = (SelectAnalysis) analyze("select * from sys.nodes limit 1 offset 3");
        assertThat(analyze.offset(), is(3));
    }

    @Test
    public void testNoMatchStatement() throws Exception {
        for (String stmt : ImmutableList.of(
                "select id from sys.nodes where false",
                "select id from sys.nodes where 1=0",
                "select id from sys.nodes where sys.cluster.name = 'something'"
        )) {
            Analysis analysis = analyze(stmt);
            assertTrue(stmt, analysis.noMatch());
            assertNull(stmt, analysis.whereClause());
        }
    }

    @Test
    public void testAllMatchStatement() throws Exception {
        for (String stmt : ImmutableList.of(
                "select id from sys.nodes where true",
                "select id from sys.nodes where 1=1",
                "select id from sys.nodes",
                "select id from sys.nodes where sys.cluster.name = 'testcluster'"
        )) {
            Analysis analysis = analyze(stmt);
            assertFalse(stmt, analysis.noMatch());
            assertNull(stmt, analysis.whereClause());
        }
    }

    @Test
    public void test1ColPrimaryKeyLiteral() throws Exception {
        Analysis analysis = analyze("select name from sys.nodes where id='jalla'");
        assertEquals(analysis.primaryKeyLiterals(), ImmutableList.<Literal>of(new StringLiteral("jalla")));

        analysis = analyze("select name from sys.nodes where 'jalla'=id");
        assertEquals(analysis.primaryKeyLiterals(), ImmutableList.<Literal>of(new StringLiteral("jalla")));


        analysis = analyze("select name from sys.nodes where id='jalla' and id='jalla'");
        assertEquals(analysis.primaryKeyLiterals(), ImmutableList.<Literal>of(new StringLiteral("jalla")));

        analysis = analyze("select name from sys.nodes where id='jalla' and (id='jalla' or 1=1)");
        assertEquals(analysis.primaryKeyLiterals(), ImmutableList.<Literal>of(new StringLiteral("jalla")));

        // a no match results in undefined key literals, since those are ambiguous
        analysis = analyze("select name from sys.nodes where id='jalla' and id='kelle'");
        assertNull(analysis.primaryKeyLiterals());
        assertTrue(analysis.noMatch());

        analysis = analyze("select name from sys.nodes where id='jalla' or name = 'something'");
        assertNull(analysis.primaryKeyLiterals());
        assertFalse(analysis.noMatch());

        analysis = analyze("select name from sys.nodes where name = 'something'");
        assertNull(analysis.primaryKeyLiterals());
        assertFalse(analysis.noMatch());

    }

    @Test
    public void test2ColPrimaryKeyLiteral() throws Exception {
        Analysis analysis = analyze("select id from sys.shards where id=1 and table_name='jalla'");
        assertEquals(ImmutableList.<Literal>of(new StringLiteral("jalla"), new IntegerLiteral(1)),
                analysis.primaryKeyLiterals());
        assertFalse(analysis.noMatch());

        analysis = analyze("select id from sys.shards where id=1 and table_name='jalla' and id=1");
        assertEquals(ImmutableList.<Literal>of(new StringLiteral("jalla"), new IntegerLiteral(1)),
                analysis.primaryKeyLiterals());
        assertFalse(analysis.noMatch());


        analysis = analyze("select id from sys.shards where id=1");
        assertNull(analysis.primaryKeyLiterals());
        assertFalse(analysis.noMatch());

        analysis = analyze("select id from sys.shards where id=1 and table_name='jalla' and id=2");
        assertTrue(analysis.noMatch());
        assertNull(analysis.primaryKeyLiterals());
    }

    @Test
    public void testGranularityWithSingleAggregation() throws Exception {
        Analysis analyze = analyze("select count(*) from sys.nodes");
        assertThat(analyze.rowGranularity(), is(RowGranularity.NODE));
    }

    @Test
    public void testWhereInSelect() throws Exception {
        Statement statement = SqlParser.createStatement("select load from sys.nodes where load['1'] in (1, 2, 4, 8, 16)");
        Analysis analysis = analyzer.analyze(statement);

        Function whereClause = analysis.whereClause();
        assertEquals(InOperator.NAME, whereClause.info().ident().name());
        assertThat(whereClause.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(whereClause.arguments().get(1), IsInstanceOf.instanceOf(SetLiteral.class));
        SetLiteral setLiteral = (SetLiteral) whereClause.arguments().get(1);
        assertEquals(setLiteral.symbolType(), SymbolType.SET_LITERAL);
        assertEquals(setLiteral.valueType(), DataType.LONG_SET);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWhereInSelectDifferentDataTypeList() throws Exception {
        Statement statement = SqlParser.createStatement("select 'found' where 1 in (1.2, 2)");
        analyzer.analyze(statement);
    }

    @Test
    public void testWhereInSelectDifferentDataTypeValue() throws Exception {
        Statement statement = SqlParser.createStatement("select 'found' where 1.2 in (1, 2)");
        Analysis analysis = analyzer.analyze(statement);
        assertTrue(analysis.noMatch());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWhereInSelectDifferentDataTypeValueUncompatibleDataTypes() throws Exception {
        Statement statement = SqlParser.createStatement("select 'found' where 1 in (1, 'foo', 2)");
        analyzer.analyze(statement);
    }

    @Test
    public void testAggregationDistinct() {
        SelectAnalysis analysis = (SelectAnalysis) analyze("select count(distinct load['1']) from sys.nodes");

        assertTrue(analysis.hasAggregates());
        assertEquals(2, analysis.functions().size());

        Function collectionCount = getFunctionByName(CollectionCountFunction.NAME, analysis.functions());
        Function collectSet = getFunctionByName(CollectSetAggregation.NAME, analysis.functions());
        assertNotNull(collectionCount);
        assertNotNull(collectSet);

        List<Symbol> args = collectionCount.arguments();
        assertEquals(1, args.size());
        Function innerFunction = (Function) args.get(0);
        assertTrue(innerFunction.info().isAggregate());
        assertEquals(innerFunction.info().ident().name(), CollectSetAggregation.NAME);
        List<Symbol> innerArguments = innerFunction.arguments();
        assertThat(innerArguments.get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(((Reference) innerArguments.get(0)).info(), IsInstanceOf.instanceOf(ReferenceInfo.class));
        ReferenceInfo refInfo = ((Reference) innerArguments.get(0)).info();
        assertThat(refInfo.ident().columnIdent().name(), is("load"));
        assertThat(refInfo.ident().columnIdent().path().get(0), is("1"));

        assertSame(collectSet, innerFunction);
    }

    private static Function getFunctionByName(String functionName, Collection c) {
        Function function = null;
        Iterator<Function> it = c.iterator();
        while (function == null && it.hasNext()) {
            Function f = it.next();
            if (f.info().ident().name().equals(functionName)) {
                function = f;
            }

        }
        return function;
    }

    @Test
    public void testDeleteWhere() throws Exception {
        Statement statement = SqlParser.createStatement("delete from sys.nodes where load['1'] = 1");
        SelectAnalysis analysis = (SelectAnalysis) analyzer.analyze(statement);
        assertTrue(analysis.isDelete());
        assertEquals(SysNodesTableInfo.IDENT, analysis.table().ident());

        assertThat(analysis.rowGranularity(), is(RowGranularity.NODE));
        assertFalse(analysis.hasGroupBy());

        Function whereClause = analysis.whereClause();
        assertEquals(EqOperator.NAME, whereClause.info().ident().name());
        assertFalse(whereClause.info().isAggregate());

        assertThat(whereClause.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(whereClause.arguments().get(1), IsInstanceOf.instanceOf(DoubleLiteral.class));

    }

    @Test
    public void testInsertWithColumns() throws Exception {
        InsertAnalysis analysis = (InsertAnalysis) analyze("insert into users (id, name) values (1, 'Trillian')");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(2));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("id"));
        assertThat(analysis.columns().get(0).valueType(), is(DataType.LONG));

        assertThat(analysis.columns().get(1).info().ident().columnIdent().name(), is("name"));
        assertThat(analysis.columns().get(1).valueType(), is(DataType.STRING));

        assertThat(analysis.values().size(), is(1));
        List<Symbol> values = analysis.values().get(0);
        assertThat(values.size(), is(2));
        assertThat(values.get(0), instanceOf(LongLiteral.class));
        assertThat(values.get(1), instanceOf(StringLiteral.class));
    }

    @Test
    public void testInsertWithTwistedColumns() throws Exception {
        InsertAnalysis analysis = (InsertAnalysis) analyze("insert into users (name, id) values ('Trillian', 2)");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(2));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("name"));
        assertThat(analysis.columns().get(0).valueType(), is(DataType.STRING));

        assertThat(analysis.columns().get(1).info().ident().columnIdent().name(), is("id"));
        assertThat(analysis.columns().get(1).valueType(), is(DataType.LONG));

        assertThat(analysis.values().size(), is(1));
        List<Symbol> values = analysis.values().get(0);
        assertThat(values.size(), is(2));
        assertThat(values.get(0), instanceOf(StringLiteral.class));
        assertThat(values.get(1), instanceOf(LongLiteral.class));
    }

    @Test(expected = IllegalStateException.class)
    public void testInsertWithColumnsAndTooManyValues() throws Exception {
        analyze("insert into users (name, id) values ('Trillian', 2, true)");
    }

    @Test(expected = IllegalStateException.class)
    public void testInsertWithColumnsAndTooLessValues() throws Exception {
        analyze("insert into users (name, id) values ('Trillian')");
    }

    @Test(expected = CrateException.class)
    public void testInsertWithWrongType() throws Exception {
        analyze("insert into users (name, id) values (1, 'Trillian')");
    }

    @Test(expected = CrateException.class)
    public void testInsertWithWrongParameterType() throws Exception {
        analyze("insert into users (name, id) values (?, ?)", new Object[]{1, true});
    }

    @Test
    public void testInsertWithConvertedTypes() throws Exception {
        InsertAnalysis analysis = (InsertAnalysis)analyze("insert into users (id, name, awesome) values (?, 'Trillian', ?)", new Object[]{1.0f, "true"});

        assertThat(analysis.columns().get(0).valueType(), is(DataType.LONG));
        assertThat(analysis.columns().get(2).valueType(), is(DataType.BOOLEAN));

        List<Symbol> valuesList = analysis.values().get(0);
        assertThat(valuesList.get(0), instanceOf(LongLiteral.class));
        assertThat(valuesList.get(2), instanceOf(BooleanLiteral.class));

    }

    @Test
    public void testInsertWithFunction() throws Exception {
        InsertAnalysis analysis = (InsertAnalysis) analyze("insert into users values (ABS(-1), 'Trillian')");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(2));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("id"));
        assertThat(analysis.columns().get(0).valueType(), is(DataType.LONG));

        assertThat(analysis.columns().get(1).info().ident().columnIdent().name(), is("name"));
        assertThat(analysis.columns().get(1).valueType(), is(DataType.STRING));

        assertThat(analysis.values().size(), is(1));
        List<Symbol> values = analysis.values().get(0);
        assertThat(values.size(), is(2));
        assertThat(values.get(0), instanceOf(Function.class));
        assertThat(values.get(1), instanceOf(StringLiteral.class));
    }

    @Test
    public void testInsertWithoutColumns() throws Exception {
        InsertAnalysis analysis = (InsertAnalysis) analyze("insert into users values (1, 'Trillian')");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(2));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("id"));
        assertThat(analysis.columns().get(0).valueType(), is(DataType.LONG));

        assertThat(analysis.columns().get(1).info().ident().columnIdent().name(), is("name"));
        assertThat(analysis.columns().get(1).valueType(), is(DataType.STRING));

        assertThat(analysis.values().size(), is(1));
        List<Symbol> values = analysis.values().get(0);
        assertThat(values.size(), is(2));
        assertThat(values.get(0), instanceOf(LongLiteral.class));
        assertThat(values.get(1), instanceOf(StringLiteral.class));
    }

    @Test
    public void testInsertWithoutColumnsAndOnlyOneColumn() throws Exception {
        InsertAnalysis analysis = (InsertAnalysis) analyze("insert into users values (1)");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(1));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("id"));
        assertThat(analysis.columns().get(0).valueType(), is(DataType.LONG));

        assertThat(analysis.values().size(), is(1));
        List<Symbol> values = analysis.values().get(0);
        assertThat(values.size(), is(1));
        assertThat(values.get(0), instanceOf(LongLiteral.class));
    }

    @Test(expected = IllegalStateException.class)
    public void testInsertIntoSysTable() throws Exception {
        analyze("insert into sys.nodes (id, name) values (666, 'evilNode')");
    }

    @Test
    public void testSelectWithObjectLiteral() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("1", 1.0);
        map.put("5", 2.5);
        map.put("15", 8.0);
        SelectAnalysis analysis = (SelectAnalysis) analyze("select id from sys.nodes where load=?",
                new Object[]{map});
        Function whereClause = analysis.whereClause();
        assertThat(whereClause.arguments().get(1), instanceOf(ObjectLiteral.class));
        assertTrue(((ObjectLiteral) whereClause.arguments().get(1)).value().equals(map));
    }

    @Test
    public void testLikeInWhereQuery() {
        Analysis analysis = analyze("select * from sys.nodes where name like 'foo'");

        assertNotNull(analysis.whereClause());
        Function whereClause = analysis.whereClause();
        assertEquals(LikeOperator.NAME, whereClause.info().ident().name());
        ImmutableList<DataType> argumentTypes = ImmutableList.<DataType>of(DataType.STRING, DataType.STRING);
        assertEquals(argumentTypes, whereClause.info().ident().argumentTypes());

        assertThat(whereClause.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(whereClause.arguments().get(1), IsInstanceOf.instanceOf(StringLiteral.class));
        StringLiteral stringLiteral = (StringLiteral) whereClause.arguments().get(1);
        assertThat(stringLiteral.value(), is("foo"));
    }

    @Test(expected = UnsupportedOperationException.class) // ESCAPE is not supported yet.
    public void testLikeEscapeInWhereQuery() {
        analyze("select * from sys.nodes where name like 'foo' escape 'o'");
    }

    @Test
    public void testLikeNoStringDataTypeInWhereQuery() {
        Analysis analysis = analyze("select * from sys.nodes where name like 1");

        // check if the implicit cast of the pattern worked
        ImmutableList<DataType> argumentTypes = ImmutableList.<DataType>of(DataType.STRING, DataType.STRING);
        Function whereClause = analysis.whereClause();
        assertEquals(argumentTypes, whereClause.info().ident().argumentTypes());
        assertThat(whereClause.arguments().get(1), IsInstanceOf.instanceOf(StringLiteral.class));
        StringLiteral stringLiteral = (StringLiteral) whereClause.arguments().get(1);
        assertThat(stringLiteral.value(), is("1"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLikeReferenceInPatternInWhereQuery() {
        analyze("select * from sys.nodes where 1 like name");
    }

    @Test
    public void testLikeLongDataTypeInWhereQuery() {
        Analysis analysis = analyze("select * from sys.nodes where 1 like 2");

        // check if implicit cast worked of both, expression and pattern.
        Function function = (Function) analysis.functions().toArray()[0];
        assertEquals(LikeOperator.NAME, function.info().ident().name());
        ImmutableList<DataType> argumentTypes = ImmutableList.<DataType>of(DataType.STRING, DataType.STRING);
        assertEquals(argumentTypes, function.info().ident().argumentTypes());

        assertThat(function.arguments().get(0), IsInstanceOf.instanceOf(StringLiteral.class));
        assertThat(function.arguments().get(1), IsInstanceOf.instanceOf(StringLiteral.class));
        StringLiteral expressionLiteral = (StringLiteral) function.arguments().get(0);
        StringLiteral patternLiteral = (StringLiteral) function.arguments().get(1);
        assertThat(expressionLiteral.value(), is("1"));
        assertThat(patternLiteral.value(), is("2"));
    }

}
