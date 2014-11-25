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
import io.crate.PartitionName;
import io.crate.metadata.*;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.Input;
import io.crate.operation.reference.sys.cluster.SysClusterExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.parser.SqlParser;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.joda.time.DateTime;
import org.junit.Before;

import java.util.*;

public abstract class BaseAnalyzerTest {

    static final Routing shardRouting = new Routing(ImmutableMap.<String, Map<String, Set<Integer>>>builder()
            .put("nodeOne", ImmutableMap.<String, Set<Integer>>of("t1", ImmutableSet.of(1, 2)))
            .put("nodeTow", ImmutableMap.<String, Set<Integer>>of("t1", ImmutableSet.of(3, 4)))
            .build());
    static final TableIdent TEST_DOC_TABLE_IDENT = new TableIdent(null, "users");
    static final TableInfo userTableInfo = TestingTableInfo.builder(TEST_DOC_TABLE_IDENT, RowGranularity.DOC, shardRouting)
            .add("id", DataTypes.LONG, null)
            .add("other_id", DataTypes.LONG, null)
            .add("name", DataTypes.STRING, null)
            .add("text", DataTypes.STRING, null, ReferenceInfo.IndexType.ANALYZED)
            .add("no_index", DataTypes.STRING, null, ReferenceInfo.IndexType.NO)
            .add("details", DataTypes.OBJECT, null)
            .add("awesome", DataTypes.BOOLEAN, null)
            .add("counters", new ArrayType(DataTypes.LONG), null)
            .add("friends", new ArrayType(DataTypes.OBJECT), null, ColumnPolicy.DYNAMIC)
            .add("friends", DataTypes.LONG, Arrays.asList("id"))
            .add("friends", new ArrayType(DataTypes.STRING), Arrays.asList("groups"))
            .add("tags", new ArrayType(DataTypes.STRING), null)
            .add("bytes", DataTypes.BYTE, null)
            .add("shorts", DataTypes.SHORT, null)
            .add("ints", DataTypes.INTEGER, null)
            .add("floats", DataTypes.FLOAT, null)
            .addIndex(ColumnIdent.fromPath("name_text_ft"), ReferenceInfo.IndexType.ANALYZED)
            .addPrimaryKey("id")
            .clusteredBy("id")
            .build();
    static final TableIdent TEST_DOC_TABLE_IDENT_MULTI_PK = new TableIdent(null, "users_multi_pk");
    static final TableInfo userTableInfoMultiPk = TestingTableInfo.builder(TEST_DOC_TABLE_IDENT_MULTI_PK, RowGranularity.DOC, shardRouting)
            .add("id", DataTypes.LONG, null)
            .add("name", DataTypes.STRING, null)
            .add("details", DataTypes.OBJECT, null)
            .add("awesome", DataTypes.BOOLEAN, null)
            .add("friends", new ArrayType(DataTypes.OBJECT), null, ColumnPolicy.DYNAMIC)
            .addPrimaryKey("id")
            .addPrimaryKey("name")
            .clusteredBy("id")
            .build();
    static final TableIdent TEST_DOC_TABLE_IDENT_CLUSTERED_BY_ONLY = new TableIdent(null, "users_clustered_by_only");
    static final TableInfo userTableInfoClusteredByOnly = TestingTableInfo.builder(TEST_DOC_TABLE_IDENT_CLUSTERED_BY_ONLY, RowGranularity.DOC, shardRouting)
            .add("id", DataTypes.LONG, null)
            .add("name", DataTypes.STRING, null)
            .add("details", DataTypes.OBJECT, null)
            .add("awesome", DataTypes.BOOLEAN, null)
            .add("friends", new ArrayType(DataTypes.OBJECT), null, ColumnPolicy.DYNAMIC)
            .clusteredBy("id")
            .build();
    static final TableIdent TEST_DOC_TABLE_REFRESH_INTERVAL_BY_ONLY = new TableIdent(null, "user_refresh_interval");
    static final TableInfo userTableInfoRefreshIntervalByOnly = TestingTableInfo.builder(TEST_DOC_TABLE_REFRESH_INTERVAL_BY_ONLY, RowGranularity.DOC, shardRouting)
            .add("id", DataTypes.LONG, null)
            .add("content", DataTypes.STRING, null)
            .clusteredBy("id")
            .build();
    static final TableIdent NESTED_PK_TABLE_IDENT = new TableIdent(null, "nested_pk");
    static final TableInfo nestedPkTableInfo = TestingTableInfo.builder(NESTED_PK_TABLE_IDENT, RowGranularity.DOC, shardRouting)
            .add("id", DataTypes.LONG, null)
            .add("o", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
            .add("o", DataTypes.BYTE, Arrays.asList("b"))
            .addPrimaryKey("id")
            .addPrimaryKey("o.b")
            .clusteredBy("o.b")
            .build();
    static final TableIdent TEST_PARTITIONED_TABLE_IDENT = new TableIdent(null, "parted");
    static final TableInfo TEST_PARTITIONED_TABLE_INFO = new TestingTableInfo.Builder(
            TEST_PARTITIONED_TABLE_IDENT, RowGranularity.DOC, new Routing())
            .add("id", DataTypes.INTEGER, null)
            .add("name", DataTypes.STRING, null)
            .add("date", DataTypes.TIMESTAMP, null, true)
            .add("obj", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
            // add 3 partitions/simulate already done inserts
            .addPartitions(
                    new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).stringValue(),
                    new PartitionName("parted", Arrays.asList(new BytesRef("1395961200000"))).stringValue(),
                    new PartitionName("parted", new ArrayList<BytesRef>(){{add(null);}}).stringValue())
            .build();
    static final TableIdent TEST_MULTIPLE_PARTITIONED_TABLE_IDENT = new TableIdent(null, "multi_parted");
    static final TableInfo TEST_MULTIPLE_PARTITIONED_TABLE_INFO = new TestingTableInfo.Builder(
            TEST_MULTIPLE_PARTITIONED_TABLE_IDENT, RowGranularity.DOC, new Routing())
            .add("id", DataTypes.INTEGER, null)
            .add("date", DataTypes.TIMESTAMP, null, true)
            .add("num", DataTypes.LONG, null)
            .add("obj", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
            .add("obj", DataTypes.STRING, Arrays.asList("name"), true)
            // add 3 partitions/simulate already done inserts
            .addPartitions(
                    new PartitionName("multi_parted", Arrays.asList(new BytesRef("1395874800000"), new BytesRef("0"))).stringValue(),
                    new PartitionName("multi_parted", Arrays.asList(new BytesRef("1395961200000"), new BytesRef("-100"))).stringValue(),
                    new PartitionName("multi_parted", Arrays.asList(null, new BytesRef("-100"))).stringValue())
            .build();
    static final TableIdent TEST_NESTED_PARTITIONED_TABLE_IDENT = new TableIdent(null, "nested_parted");
    static final TableInfo TEST_NESTED_PARTITIONED_TABLE_INFO = new TestingTableInfo.Builder(
            TEST_NESTED_PARTITIONED_TABLE_IDENT, RowGranularity.DOC, new Routing())
            .add("id", DataTypes.INTEGER, null)
            .add("date", DataTypes.TIMESTAMP, null, true)
            .add("obj", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
            .add("obj", DataTypes.STRING, Arrays.asList("name"), true)
                    // add 3 partitions/simulate already done inserts
            .addPartitions(
                    new PartitionName("nested_parted", Arrays.asList(new BytesRef("1395874800000"), new BytesRef("Trillian"))).stringValue(),
                    new PartitionName("nested_parted", Arrays.asList(new BytesRef("1395961200000"), new BytesRef("Ford"))).stringValue(),
                    new PartitionName("nested_parted", Arrays.asList(null, new BytesRef("Zaphod"))).stringValue())
            .build();
    static final TableIdent TEST_DOC_TRANSACTIONS_TABLE_IDENT = new TableIdent(null, "transactions");
    static final TableInfo TEST_DOC_TRANSACTIONS_TABLE_INFO = new TestingTableInfo.Builder(
            TEST_DOC_TRANSACTIONS_TABLE_IDENT, RowGranularity.DOC, new Routing())
            .add("id", DataTypes.LONG, null)
            .add("sender", DataTypes.STRING, null)
            .add("recipient", DataTypes.STRING, null)
            .add("amount", DataTypes.DOUBLE, null)
            .add("timestamp", DataTypes.TIMESTAMP, null)
            .build();
    static final TableIdent DEEPLY_NESTED_TABLE_IDENT = new TableIdent(null, "deeply_nested");
    static final TableInfo DEEPLY_NESTED_TABLE_INFO = new TestingTableInfo.Builder(
            DEEPLY_NESTED_TABLE_IDENT, RowGranularity.DOC, new Routing())
            .add("details", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
            .add("details", DataTypes.BOOLEAN, Arrays.asList("awesome"))
            .add("details", DataTypes.OBJECT, Arrays.asList("stuff"), ColumnPolicy.DYNAMIC)
            .add("details", DataTypes.STRING, Arrays.asList("stuff", "name"))
            .add("details", new ArrayType(DataTypes.OBJECT), Arrays.asList("arguments"))
            .add("details", DataTypes.DOUBLE, Arrays.asList("arguments", "quality"))
            .add("details", DataTypes.STRING, Arrays.asList("arguments", "name"))
            .add("tags", new ArrayType(DataTypes.OBJECT), null)
            .add("tags", DataTypes.STRING, Arrays.asList("name"))
            .add("tags", new ArrayType(DataTypes.OBJECT), Arrays.asList("metadata"))
            .add("tags", DataTypes.LONG, Arrays.asList("metadata", "id"))
            .build();

    static final TableIdent TEST_DOC_LOCATIONS_TABLE_IDENT = new TableIdent(null, "locations");
    static final TableInfo TEST_DOC_LOCATIONS_TABLE_INFO = TestingTableInfo.builder(TEST_DOC_LOCATIONS_TABLE_IDENT, RowGranularity.DOC, shardRouting)
            .add("id", DataTypes.LONG, null)
            .add("loc", DataTypes.GEO_POINT, null)
            .build();

    static final TableInfo TEST_CLUSTER_BY_STRING_TABLE_INFO = TestingTableInfo.builder(new TableIdent(null, "bystring"), RowGranularity.DOC, shardRouting)
            .add("name", DataTypes.STRING, null)
            .add("score", DataTypes.DOUBLE, null)
            .addPrimaryKey("name")
            .clusteredBy("name")
            .build();

    static final TableIdent TEST_BLOB_TABLE_IDENT = new TableIdent(null, "myblobs");
    static final TableInfo TEST_BLOB_TABLE_TABLE_INFO = TestingTableInfo.builder(TEST_BLOB_TABLE_IDENT, RowGranularity.DOC, shardRouting)
            .build();

    static final FunctionInfo ABS_FUNCTION_INFO = new FunctionInfo(
            new FunctionIdent("abs", Arrays.<DataType>asList(DataTypes.LONG)),
            DataTypes.LONG);
    static final FunctionInfo YEAR_FUNCTION_INFO = new FunctionInfo(
            new FunctionIdent("year", Arrays.<DataType>asList(DataTypes.TIMESTAMP)),
            DataTypes.STRING);

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

    static class AbsFunction extends Scalar<Long, Number> {

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
                return Literal.newLiteral(evaluate((Input<Number>) symbol.arguments().get(0)));
            }
            return symbol;
        }
    }

    static class YearFunction extends Scalar<String, Long> {

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
                return Literal.newLiteral(evaluate((Input<Long>) symbol.arguments().get(0)));
            }
            return symbol;
        }
    }

    protected AnalyzedStatement analyze(String statement) {
        return analyzer.analyze(SqlParser.createStatement(statement)).analyzedStatement();
    }

    protected AnalyzedStatement analyze(String statement, Object[] params) {
        return analyzer.analyze(SqlParser.createStatement(statement), params, new Object[0][]).analyzedStatement();
    }

    protected AnalyzedStatement analyze(String statement, Object[][] bulkArgs) {
        return analyzer.analyze(SqlParser.createStatement(statement), new Object[0], bulkArgs).analyzedStatement();
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
