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
import io.crate.operator.operator.EqOperator;
import io.crate.operator.operator.LteOperator;
import io.crate.operator.operator.OperatorModule;
import io.crate.operator.operator.OrOperator;
import io.crate.operator.reference.sys.node.NodeLoadExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.DoubleLiteral;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.os.OsStats;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
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
        }
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
        assertThat(left.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(left.arguments().get(1), IsInstanceOf.instanceOf(DoubleLiteral.class));
    }
}
