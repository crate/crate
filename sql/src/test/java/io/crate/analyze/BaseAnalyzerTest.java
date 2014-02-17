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
import com.google.common.collect.ImmutableSet;
import io.crate.metadata.*;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operator.Input;
import io.crate.operator.reference.sys.cluster.SysClusterExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.LongLiteral;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.parser.SqlParser;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.os.OsStats;
import org.junit.Before;

import java.util.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaseAnalyzerTest {

    static final Routing shardRouting = new Routing(ImmutableMap.<String, Map<String, Set<Integer>>>builder()
            .put("nodeOne", ImmutableMap.<String, Set<Integer>>of("t1", ImmutableSet.of(1, 2)))
            .put("nodeTow", ImmutableMap.<String, Set<Integer>>of("t1", ImmutableSet.of(3, 4)))
            .build());
    static final TableIdent TEST_DOC_TABLE_IDENT = new TableIdent(null, "users");
    static final TableInfo userTableInfo = TestingTableInfo.builder(TEST_DOC_TABLE_IDENT, RowGranularity.DOC, shardRouting)
            .add("id", DataType.LONG, null)
            .add("name", DataType.STRING, null)
            .add("details", DataType.OBJECT, null)
            .add("awesome", DataType.BOOLEAN, null)
            .addPrimaryKey("id")
            .build();
    static final FunctionInfo ABS_FUNCTION_INFO = new FunctionInfo(
            new FunctionIdent("abs", Arrays.asList(DataType.LONG)),
            DataType.LONG);
    Injector injector;
    Analyzer analyzer;

    static final ReferenceInfo LOAD_INFO = SysNodesTableInfo.INFOS.get(new ColumnIdent("load"));
    static final ReferenceInfo LOAD1_INFO = SysNodesTableInfo.INFOS.get(new ColumnIdent("load", "1"));
    static final ReferenceInfo LOAD5_INFO = SysNodesTableInfo.INFOS.get(new ColumnIdent("load", "5"));

    static final ReferenceInfo CLUSTER_NAME_INFO = SysClusterTableInfo.INFOS.get(new ColumnIdent("name"));

    static class ClusterNameExpression extends SysClusterExpression<BytesRef> {

        protected ClusterNameExpression() {
            super(CLUSTER_NAME_INFO.ident().columnIdent().name());
        }

        @Override
        public BytesRef value() {
            return new BytesRef("testcluster");
        }
    }

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
            if (symbol.arguments().get(0) instanceof Input) {
                return new LongLiteral(evaluate((Input<?>)symbol.arguments().get(0)));
            }
            return symbol;
        }
    }

    /**
     * borrowed from {@link io.crate.operator.reference.sys.TestGlobalSysExpressions}
     * // TODO share it
     */
    static class TestModule extends AbstractModule {

        @Override
        protected void configure() {
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

    protected Analysis analyze(String statement) {
        return analyzer.analyze(SqlParser.createStatement(statement));
    }

    protected Analysis analyze(String statement, Object[] params) {
        return analyzer.analyze(SqlParser.createStatement(statement), params);
    }

    protected List<Module> getModules() {
        return new ArrayList<>();
    }

    @Before
    public void setUp() throws Exception {
        ModulesBuilder builder = new ModulesBuilder();
        for (Module m : getModules()) {
            builder.add(m);
        }
        injector = builder.createInjector();
        analyzer = injector.getInstance(Analyzer.class);
    }



    protected static Function getFunctionByName(String functionName, Collection c) {
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
}
