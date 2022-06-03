/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner;

import static io.crate.planner.operators.LogicalPlannerTest.isPlan;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RuleSets;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.data.Row;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.LogicalPlan;
import io.crate.sql.tree.QualifiedName;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class CalcitePlannerTest extends CrateDummyClusterServiceUnitTest {

    public static final Set<RelOptRule> RULE_SET = Set.of(
        EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE,
        EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
        EnumerableRules.ENUMERABLE_CORRELATE_RULE,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_FILTER_RULE,
        EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
        EnumerableRules.ENUMERABLE_SORT_RULE,
        EnumerableRules.ENUMERABLE_LIMIT_RULE,
        EnumerableRules.ENUMERABLE_UNION_RULE,
        EnumerableRules.ENUMERABLE_MERGE_UNION_RULE,
        EnumerableRules.ENUMERABLE_INTERSECT_RULE,
        EnumerableRules.ENUMERABLE_MINUS_RULE,
        EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
        EnumerableRules.ENUMERABLE_VALUES_RULE,
        EnumerableRules.ENUMERABLE_WINDOW_RULE,
        EnumerableRules.ENUMERABLE_MATCH_RULE,
        CoreRules.PROJECT_TO_SEMI_JOIN,
        CoreRules.JOIN_TO_SEMI_JOIN,
        CoreRules.MATCH,
        CalciteSystemProperty.COMMUTE.value()
            ? CoreRules.JOIN_ASSOCIATE
            : CoreRules.PROJECT_MERGE,
        CoreRules.AGGREGATE_STAR_TABLE,
        CoreRules.AGGREGATE_PROJECT_STAR_TABLE,
        CoreRules.FILTER_SCAN,
        CoreRules.FILTER_PROJECT_TRANSPOSE,
        CoreRules.FILTER_INTO_JOIN,
        CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
        CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
        CoreRules.FILTER_AGGREGATE_TRANSPOSE,
        CoreRules.JOIN_COMMUTE,
        JoinPushThroughJoinRule.RIGHT,
        JoinPushThroughJoinRule.LEFT,
        CoreRules.SORT_PROJECT_TRANSPOSE
    );

    private static final Schema CRATE_DB_SCHEMA = new Schema() {

        final Table table = new Table() {
            /**
             * {@inheritDoc}
             *
             * <p>Table schema is as follows:
             *
             * <pre>{@code
             * myTable(
             *      a: BIGINT,
             *      n1: STRUCT<
             *            n11: STRUCT<b: BIGINT>,
             *            n12: STRUCT<c: BIGINT>
             *          >,
             *      n2: STRUCT<d: BIGINT>,
             *      e: BIGINT)
             * }</pre>
             */
            @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
                return typeFactory.builder()
                    .add("a", bigint)
                    .add("n1",
                        typeFactory.builder()
                            .add("n11", typeFactory.builder().add("b", bigint).build())
                            .add("n12", typeFactory.builder().add("c", bigint).build())
                            .build())
                    .add("n2", typeFactory.builder().add("d", bigint).build())
                    .add("e", bigint)
                    .build();
            }

            @Override public Statistic getStatistic() {
                return new Statistic() {
                    @Override public Double getRowCount() {
                        return 2D;
                    }
                };
            }

            @Override public Schema.TableType getJdbcTableType() {
                return null;
            }

            @Override public boolean isRolledUp(String column) {
                return false;
            }

            @Override public boolean rolledUpColumnValidInsideAgg(String column,
                                                                  SqlCall call, @Nullable SqlNode parent,
                                                                  @Nullable CalciteConnectionConfig config) {
                return false;
            }
        };

        @Override public Table getTable(String name) {
            return table;
        }

        @Override public Set<String> getTableNames() {
            return ImmutableSet.of("t1");
        }

        @Override public RelProtoDataType getType(String name) {
            return null;
        }

        @Override public Set<String> getTypeNames() {
            return ImmutableSet.of();
        }

        @Override public Collection<Function> getFunctions(String name) {
            return null;
        }

        @Override public Set<String> getFunctionNames() {
            return ImmutableSet.of();
        }

        @Override public Schema getSubSchema(String name) {
            return null;
        }

        @Override public Set<String> getSubSchemaNames() {
            return ImmutableSet.of();
        }

        @Override public Expression getExpression(@Nullable SchemaPlus parentSchema,
                                                  String name) {
            return null;
        }

        @Override public boolean isMutable() {
            return false;
        }

        @Override public Schema snapshot(SchemaVersion version) {
            return null;
        }
    };

    public static Frameworks.ConfigBuilder config() {
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        return Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT)
            .defaultSchema(rootSchema.add("doc", CRATE_DB_SCHEMA))
            .traitDefs((List<RelTraitDef>) null)
            .programs(Programs.heuristicJoinOrder(RULE_SET, true, 2));
    }

    private FrameworkConfig config;
    private SQLExecutor sqlExecutor;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        config = config().build();
        sqlExecutor = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE t1 (" +
                " a BIGINT," +
                " n1 OBJECT AS (n11 OBJECT AS (b BIGINT), n12 OBJECT AS (c BIGINT))," +
                " n2 OBJECT AS (d BIGINT)," +
                " e BIGINT" +
                ")")
            .build();
    }

    public LogicalPlan convert(RelNode relNode) {
        if (relNode instanceof LogicalTableScan tableScan) {
            var tableName = QualifiedName.of(tableScan.getTable().getQualifiedName()).toString();
            DocTableInfo docTableInfo = sqlExecutor.resolveTableInfo(tableName);
            DocTableRelation tableRelation = new DocTableRelation(docTableInfo);
            return Collect.create(
                tableRelation,
                tableRelation.outputs(),
                WhereClause.MATCH_ALL,
                new TableStats(),
                Row.EMPTY
            );
        }
        return null;
    }


    @Test
    public void test_select_star() {
        RelBuilder builder = RelBuilder.create(config);
        RelNode relNode = builder
            .scan("t1")
            .build();

        System.out.println(RelOptUtil.toString(relNode));

        var logicalPlan = convert(relNode);
        assertThat(logicalPlan, isPlan("Collect[doc.t1 | [a, n1, n2, e] | true]"));
    }

    @Test
    public void test_optimizer() {
        RelBuilder builder = RelBuilder.create(config);
        RelNode logicalPlan = builder
            .scan("t1")
            .project(builder.field("a"), builder.field("n1"))
            .filter(
                builder.call(SqlStdOperatorTable.GREATER_THAN,
                builder.field("a"),
                builder.literal(10))
            )
            .build();

        System.out.println(RelOptUtil.toString(logicalPlan));

        var optimizedPlan = optimize(logicalPlan);
        System.out.println(RelOptUtil.toString(optimizedPlan));
    }

    private RelNode optimize(RelNode logicalPlan) {
        RelOptCluster cluster = logicalPlan.getCluster();
        final RelOptPlanner optPlanner = cluster.getPlanner();

        RelTraitSet desiredTraits =
            cluster.traitSet()
                .replace(EnumerableConvention.INSTANCE);
        final RelCollation collation =
            logicalPlan instanceof Sort
                ? ((Sort) logicalPlan).collation
                : null;
        if (collation != null) {
            desiredTraits = desiredTraits.replace(collation);
        }
        RelNode newRoot = optPlanner.changeTraits(logicalPlan, desiredTraits);
        optPlanner.setRoot(newRoot);
        return optPlanner.findBestExp();
    }
}
