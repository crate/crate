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
import io.crate.DataType;
import io.crate.PartitionName;
import io.crate.metadata.*;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.Input;
import io.crate.operation.reference.sys.cluster.SysClusterExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.LongLiteral;
import io.crate.planner.symbol.StringLiteral;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.parser.SqlParser;
import org.apache.lucene.util.BytesRef;
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
import org.joda.time.DateTime;
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
            .add("other_id", DataType.LONG, null)
            .add("name", DataType.STRING, null)
            .add("details", DataType.OBJECT, null)
            .add("awesome", DataType.BOOLEAN, null)
            .add("_version", DataType.INTEGER, null)
            .add("counters", DataType.LONG_ARRAY, null)
            .add("friends", DataType.OBJECT_ARRAY, null, ReferenceInfo.ObjectType.DYNAMIC)
            .add("friends", DataType.LONG, Arrays.asList("id"))
            .addPrimaryKey("id")
            .clusteredBy("id")
            .build();
    static final TableIdent TEST_DOC_TABLE_IDENT_MULTI_PK = new TableIdent(null, "users_multi_pk");
    static final TableInfo userTableInfoMultiPk = TestingTableInfo.builder(TEST_DOC_TABLE_IDENT_MULTI_PK, RowGranularity.DOC, shardRouting)
            .add("id", DataType.LONG, null)
            .add("name", DataType.STRING, null)
            .add("details", DataType.OBJECT, null)
            .add("awesome", DataType.BOOLEAN, null)
            .add("_version", DataType.INTEGER, null)
            .add("friends", DataType.OBJECT_ARRAY, null, ReferenceInfo.ObjectType.DYNAMIC)
            .addPrimaryKey("id")
            .addPrimaryKey("name")
            .clusteredBy("id")
            .build();
    static final TableIdent TEST_DOC_TABLE_IDENT_CLUSTERED_BY_ONLY = new TableIdent(null, "users_clustered_by_only");
    static final TableInfo userTableInfoClusteredByOnly = TestingTableInfo.builder(TEST_DOC_TABLE_IDENT_CLUSTERED_BY_ONLY, RowGranularity.DOC, shardRouting)
            .add("id", DataType.LONG, null)
            .add("name", DataType.STRING, null)
            .add("details", DataType.OBJECT, null)
            .add("awesome", DataType.BOOLEAN, null)
            .add("_version", DataType.INTEGER, null)
            .add("friends", DataType.OBJECT_ARRAY, null, ReferenceInfo.ObjectType.DYNAMIC)
            .clusteredBy("id")
            .build();
    static final TableIdent TEST_DOC_TABLE_REFRESH_INTERVAL_BY_ONLY = new TableIdent(null, "user_refresh_interval");
    static final TableInfo userTableInfoRefreshIntervalByOnly = TestingTableInfo.builder(TEST_DOC_TABLE_REFRESH_INTERVAL_BY_ONLY, RowGranularity.DOC, shardRouting)
            .add("id", DataType.LONG, null)
            .add("content", DataType.STRING, null)
            .clusteredBy("id")
            .build();
    static final TableIdent NESTED_PK_TABLE_IDENT = new TableIdent(null, "nested_pk");
    static final TableInfo nestedPkTableInfo = TestingTableInfo.builder(NESTED_PK_TABLE_IDENT, RowGranularity.DOC, shardRouting)
            .add("id", DataType.LONG, null)
            .add("o", DataType.OBJECT, null, ReferenceInfo.ObjectType.DYNAMIC)
            .add("o", DataType.BYTE, Arrays.asList("b"))
            .addPrimaryKey("id")
            .addPrimaryKey("o.b")
            .clusteredBy("o.b")
            .build();
    static final TableIdent TEST_PARTITIONED_TABLE_IDENT = new TableIdent(null, "parted");
    static final TableInfo TEST_PARTITIONED_TABLE_INFO = new TestingTableInfo.Builder(
            TEST_PARTITIONED_TABLE_IDENT, RowGranularity.DOC, new Routing())
            .add("id", DataType.INTEGER, null)
            .add("name", DataType.STRING, null)
            .add("date", DataType.TIMESTAMP, null, true)
            .add("obj", DataType.OBJECT, null, ReferenceInfo.ObjectType.DYNAMIC)
            // add 2 partitions/simulate already done inserts
            .addPartitions(
                    new PartitionName("parted", Arrays.asList("1395874800000")).stringValue(),
                    new PartitionName("parted", Arrays.asList("1395961200000")).stringValue(),
                    new PartitionName("parted", new ArrayList<String>(){{add(null);}}).stringValue())
            .build();
    static final TableIdent TEST_MULTIPLE_PARTITIONED_TABLE_IDENT = new TableIdent(null, "multi_parted");
    static final TableInfo TEST_MULTIPLE_PARTITIONED_TABLE_INFO = new TestingTableInfo.Builder(
            TEST_MULTIPLE_PARTITIONED_TABLE_IDENT, RowGranularity.DOC, new Routing())
            .add("id", DataType.INTEGER, null)
            .add("date", DataType.TIMESTAMP, null, true)
            .add("num", DataType.LONG, null)
            .add("obj", DataType.OBJECT, null, ReferenceInfo.ObjectType.DYNAMIC)
            .add("obj", DataType.STRING, Arrays.asList("name"), true)
                    // add 2 partitions/simulate already done inserts
            .addPartitions(
                    new PartitionName("multi_parted", Arrays.asList("1395874800000", "0")).stringValue(),
                    new PartitionName("multi_parted", Arrays.asList("1395961200000", "-100")).stringValue(),
                    new PartitionName("multi_parted", Arrays.asList(null, "-100")).stringValue())
            .build();

    static final FunctionInfo ABS_FUNCTION_INFO = new FunctionInfo(
            new FunctionIdent("abs", Arrays.asList(DataType.LONG)),
            DataType.LONG);
    static final FunctionInfo YEAR_FUNCTION_INFO = new FunctionInfo(
            new FunctionIdent("year", Arrays.asList(DataType.TIMESTAMP)),
            DataType.STRING);

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

    static class AbsFunction implements Scalar<Long, Number> {

        @Override
        public Long evaluate(Input<Number>... args) {
            if (args[0].value() == null) {
                return null;
            }
            return Math.abs((args[0].value()).longValue());
        }

        @Override
        public FunctionInfo info() {
            return ABS_FUNCTION_INFO;
        }


        @Override
        public Symbol normalizeSymbol(Function symbol) {
            if (symbol.arguments().get(0) instanceof Input) {
                return new LongLiteral(evaluate((Input<Number>)symbol.arguments().get(0)));
            }
            return symbol;
        }
    }

    static class YearFunction implements Scalar<String, Long> {

        @Override
        public String evaluate(Input<Long>... args) {
            if (args == null || args.length == 0 || args[0] == null) {
                return null;
            }
            return new DateTime(args[0]).year().getAsString();
        }

        @Override
        public FunctionInfo info() {
            return YEAR_FUNCTION_INFO;
        }

        @Override
        public Symbol normalizeSymbol(Function symbol) {
            if (symbol.arguments().get(0) instanceof Input) {
                return new StringLiteral(evaluate((Input<Long>)symbol.arguments().get(0)));
            }
            return symbol;
        }
    }

    /**
     * borrowed from {@link io.crate.operation.reference.sys.TestGlobalSysExpressions}
     * // TODO share it
     */
    static class TestModule extends AbstractModule {

        @Override
        protected void configure() {
            ClusterService clusterService = mock(ClusterService.class);
            ClusterState state = mock(ClusterState.class);
            MetaData metaData = mock(MetaData.class);
            when(metaData.settings()).thenReturn(ImmutableSettings.EMPTY);
            when(metaData.persistentSettings()).thenReturn(ImmutableSettings.EMPTY);
            when(metaData.transientSettings()).thenReturn(ImmutableSettings.EMPTY);
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
