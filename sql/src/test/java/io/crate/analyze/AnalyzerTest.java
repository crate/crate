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

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.operator.aggregation.impl.AggregationImplModule;
import io.crate.operator.aggregation.impl.AverageAggregation;
import io.crate.operator.operator.*;
import io.crate.operator.reference.sys.node.NodeLoadExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import org.cratedb.DataType;
import org.cratedb.sql.AmbiguousAliasException;
import org.elasticsearch.cluster.ClusterService;
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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AnalyzerTest {

    private Injector injector;
    private Analyzer analyzer;

    private static final ReferenceInfo LOAD_INFO = SysNodesTableInfo.INFOS.get(new ColumnIdent("load"));
    private static final ReferenceInfo LOAD1_INFO = SysNodesTableInfo.INFOS.get(new ColumnIdent("load", "1"));
    private static final ReferenceInfo LOAD5_INFO = SysNodesTableInfo.INFOS.get(new ColumnIdent("load", "5"));


    class TestMetaDataModule extends MetaDataModule {

        @Override
        protected void bindReferences() {
            super.bindReferences();
            referenceBinder.addBinding(LOAD_INFO.ident()).to(NodeLoadExpression.class).asEagerSingleton();
        }

        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            bind(SchemaInfo.class).toInstance(schemaInfo);
        }
    }

    /**
     * borrowed from {@link io.crate.operator.reference.sys.TestGlobalSysExpressions}
     * // TODO share it
     */
    class TestModule extends AbstractModule {

        @Override
        protected void configure() {
            ClusterService clusterService = mock(ClusterService.class);
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

    @Before
    public void setUp() throws Exception {
        injector = new ModulesBuilder()
                .add(new TestModule())
                .add(new TestMetaDataModule())
                .add(new MetaDataSysModule())
                .add(new AggregationImplModule())
                .add(new OperatorModule())
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
        Analysis analysis = analyzer.analyze(statement);
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
    public void testGroupedSelect() throws Exception {
        Statement statement = SqlParser.createStatement("select load['1'],load['5'] from sys.nodes group by load['1']");
        Analysis analysis = analyzer.analyze(statement);
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
        Analysis analysis = analyzer.analyze(statement);
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
        Analysis analysis = analyzer.analyze(statement);
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
        Analysis analysis = analyzer.analyze(statement);
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
        Analysis analyze = analyze("select load as l from sys.nodes order by l");
        assertThat(analyze.outputNames().size(), is(1));
        assertThat(analyze.outputNames().get(0), is("l"));

        assertTrue(analyze.isSorted());
        assertThat(analyze.sortSymbols().size(), is(1));
        assertThat(analyze.sortSymbols().get(0), is(analyze.outputSymbols().get(0)));
    }

    @Test (expected = AmbiguousAliasException.class)
    public void testAmbiguousOrderByOnAlias() throws Exception {
        analyze("select id as load, load from sys.nodes order by load");
    }

    @Test
    public void testOffsetSupportInAnalyzer() throws Exception {
        Analysis analyze = analyze("select * from sys.nodes limit 1 offset 3");
        assertThat(analyze.offset(), is(3));
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

        Function whereClause = analysis.whereClause();
        assertEquals(InOperator.NAME, whereClause.info().ident().name());
        assertThat(whereClause.arguments().get(0), IsInstanceOf.instanceOf(DoubleLiteral.class));
        assertThat(whereClause.arguments().get(1), IsInstanceOf.instanceOf(SetLiteral.class));
        SetLiteral setLiteral = (SetLiteral) whereClause.arguments().get(1);
        assertEquals(setLiteral.symbolType(), SymbolType.SET_LITERAL);
        assertEquals(setLiteral.valueType(), DataType.LONG_SET);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWhereInSelectDifferentDataTypeValueUncompatibleDataTypes() throws Exception {
        Statement statement = SqlParser.createStatement("select 'found' where 1 in (1, 'foo', 2)");
        analyzer.analyze(statement);
    }

}
