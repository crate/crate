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

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.Routing;
import io.crate.metadata.Routings;
import io.crate.metadata.TableIdent;
import io.crate.metadata.sys.SystemReferences;
import io.crate.operator.aggregation.impl.AggregationImplModule;
import io.crate.operator.aggregation.impl.AverageAggregation;
import io.crate.operator.reference.sys.NodeLoadExpression;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.os.OsStats;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AnalyzerTest {

    private Injector injector;
    private Analyzer analyzer;

    class TestMetaDataModule extends MetaDataModule {

        @Override
        protected void bindRoutings() {
            Map<String, Map<String, Integer>> locations = ImmutableMap.<String, Map<String, Integer>>builder()
                    .put("nodeOne", ImmutableMap.<String, Integer>of())
                    .put("nodeTwo", ImmutableMap.<String, Integer>of())
                    .build();
            final Routing routing = new Routing(locations);

            Routings routings = new Routings() {

                @Override
                public Routing getRouting(TableIdent tableIdent) {
                    return routing;
                }
            };
            bind(Routings.class).toInstance(routings);
        }

        @Override
        protected void bindReferences() {
            super.bindReferences();
            referenceBinder.addBinding(NodeLoadExpression.INFO_LOAD.ident()).to(NodeLoadExpression.class).asEagerSingleton();
        }
    }

    /**
     * borrowed from {@link io.crate.operator.reference.sys.TestGlobalSysExpressions}
     * // TODO share it
     */
    class TestModule extends AbstractModule {

        @Override
        protected void configure() {


            bind(Settings.class).toInstance(ImmutableSettings.EMPTY);

//            ClusterService cs = mock(ClusterService.class);
//            ClusterState clusterState = mock(ClusterState.class);
//            bind(ClusterService.class).toInstance(cs);
//            when(cs.state()).thenReturn(clusterState);
            //bind(RoutingsService.class).toInstance(routings);

            //DiscoveryNodes dn = mock(DiscoveryNodes.class);

            //when(clusterState.nodes()).thenReturn(dn);

            //when(dn.iterator()).theReturn(Iterators.)


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
                .add(new AggregationImplModule())
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
        assertTrue(analysis.routing().hasLocations());
        assertNull(analysis.limit());

        assertFalse(analysis.hasGroupBy());
        assertTrue(analysis.isSorted());


        assertEquals(1, analysis.outputSymbols().size());
        assertEquals(1, analysis.sortSymbols().size());
        assertEquals(1, analysis.reverseFlags().size());

        assertEquals(NodeLoadExpression.INFO_LOAD_5, ((Reference) analysis.sortSymbols().get(0)).info());

    }


    @Test
    public void testGroupedSelect() throws Exception {
        Statement statement = SqlParser.createStatement("select load['1'],load['5'] from sys.nodes group by load['1']");
        Analysis analysis = analyzer.analyze(statement);
        assertTrue(analysis.routing().hasLocations());
        assertNull(analysis.limit());

        assertTrue(analysis.hasGroupBy());
        assertEquals(2, analysis.outputSymbols().size());
        assertEquals(1, analysis.groupBy().size());
        assertEquals(NodeLoadExpression.INFO_LOAD_1, ((Reference) analysis.groupBy().get(0)).info());

    }


    @Test
    public void testSimpleSelect() throws Exception {
        Statement statement = SqlParser.createStatement("select load['5'] from sys.nodes limit 2");
        Analysis analysis = analyzer.analyze(statement);
        assertTrue(analysis.routing().hasLocations());
        assertEquals(new Integer(2), analysis.limit());

        assertFalse(analysis.hasGroupBy());


        assertEquals(SystemReferences.NODES_IDENT, analysis.table());
        assertEquals(1, analysis.outputSymbols().size());
        Reference col1 = (Reference) analysis.outputSymbols().get(0);
        assertEquals(NodeLoadExpression.INFO_LOAD_5, col1.info());

    }

    @Test
    public void testAggregationSelect() throws Exception {
        Statement statement = SqlParser.createStatement("select avg(load['5']) from sys.nodes");
        Analysis analysis = analyzer.analyze(statement);
        assertTrue(analysis.routing().hasLocations());
        assertFalse(analysis.hasGroupBy());
        assertEquals(1, analysis.outputSymbols().size());
        Function col1 = (Function) analysis.outputSymbols().get(0);
        assertTrue(col1.info().isAggregate());
        assertEquals(AverageAggregation.NAME, col1.info().ident().name());

    }


}
