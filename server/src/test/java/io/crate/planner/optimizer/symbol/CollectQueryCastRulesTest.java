/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.optimizer.symbol;

import static io.crate.testing.Asserts.assertThat;
import static java.util.Collections.emptyMap;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.WhereClause;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.LimitAndOffset;
import io.crate.expression.operator.Operator;
import io.crate.expression.operator.Operators;
import io.crate.expression.operator.any.AnyOperator;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.ComparisonExpression;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;

public class CollectQueryCastRulesTest extends CrateDummyClusterServiceUnitTest {

    private SqlExpressions e;
    private AbstractTableRelation<?> tr1;
    private PlannerContext plannerContext;


    @Before
    public void setUp() throws Exception {
        super.setUp();
        String createTableStmt =
            "create table t1 (" +
            "  id integer," +
            "  name text," +
            "  d_array array(double)," +
            "  y_array array(bigint)," +
            "  text_array array(text)," +
            "  ts_array array(timestamp with time zone)," +
            "  o_array array(object as (xs array(integer)))," +
            "  addr ip" +
            ")";
        RelationName name = new RelationName(DocSchemaInfo.NAME, "t1");
        DocTableInfo tableInfo = SQLExecutor.tableInfo(
            name,
            createTableStmt,
            clusterService);
        Map<RelationName, AnalyzedRelation> sources = Map.of(name, new TableRelation(tableInfo));
        e = new SqlExpressions(sources);
        tr1 = (AbstractTableRelation<?>) sources.get(tableInfo.ident());
        plannerContext = SQLExecutor.builder(clusterService).build().getPlannerContext(clusterService.state());
    }

    private Symbol toQuery(String query) {
        var collect = new Collect(
            tr1,
            Collections.emptyList(),
            new WhereClause(e.asSymbol(query))
        );
        var plan = (io.crate.planner.node.dql.Collect) collect.build(
            mock(DependencyCarrier.class),
            plannerContext,
            Set.of(),
            new ProjectionBuilder(e.nodeCtx),
            LimitAndOffset.NO_LIMIT,
            0,
            null,
            null,
            Row.EMPTY,
            new SubQueryResults(emptyMap()) {
                @Override
                public Object getSafe(SelectSymbol key) {
                    return Literal.of(key.valueType(), null);
                }
            }

        );
        return ((RoutedCollectPhase) plan.collectPhase()).where();
    }


    @Test
    public void test_any_operator_cast_on_left_reference_is_moved_to_cast_on_literal() {
        for (var op : AnyOperator.SUPPORTED_COMPARISONS) {
            assertThat(toQuery("name " + op + " ANY([1, 2, 2])"))
                .isFunction("any_" + op,
                    arg1 -> assertThat(arg1).isReference().hasName("name"),
                    arg2 -> assertThat(arg2).isFunction("_cast")
                );
        }
        for (var op : List.of("LIKE", "ILIKE")) {
            String lowerOp = op.toLowerCase(Locale.ENGLISH);
            assertThat(toQuery("name " + op + " ANY(d_array)"))
                .isFunction("any_" + lowerOp,
                    arg1 -> assertThat(arg1).isReference().hasName("name"),
                    arg2 -> assertThat(arg2).isFunction("_cast")
                );
            assertThat(toQuery("name NOT " + op + " ANY(d_array)"))
                .isFunction("any_not_" + lowerOp,
                    arg1 -> assertThat(arg1).isReference().hasName("name"),
                    arg2 -> assertThat(arg2).isFunction("_cast")
                );
        }
    }

    @Test
    public void test_any_operator_cast_on_right_reference_is_moved_to_cast_on_literal() {
        for (var op : AnyOperator.SUPPORTED_COMPARISONS) {
            assertThat(toQuery("'1' " + op + " ANY(d_array)")).isFunction(
                "any_" + op,
                arg1 -> assertThat(arg1).isLiteral(1.0),
                arg2 -> assertThat(arg2).isReference().hasName("d_array")
            );
        }
        for (var op : List.of("LIKE", "ILIKE")) {
            String opLower = op.toLowerCase(Locale.ENGLISH);
            assertThat(toQuery("id " + op + " ANY(text_array)")).isFunction(
                "any_" + opLower,
                arg1 -> assertThat(arg1).isFunction("_cast"),
                arg2 -> assertThat(arg2).isReference().hasName("text_array")
            );
            assertThat(toQuery("id NOT " + op + " ANY(text_array)")).isFunction(
                "any_not_" + opLower,
                arg1 -> assertThat(arg1).isFunction("_cast"),
                arg2 -> assertThat(arg2).isReference().hasName("text_array")
            );
        }
    }

    @Test
    public void test_any_operator_cast_on_nested_array_referewence_is_moved_to_cast_on_literal() {
        assertThat(toQuery("[1.0, 2.0, 3.0] = any(o_array['xs'])")).isFunction(
            "any_=",
            arg1 -> assertThat(arg1).isFunction("_cast"),
            arg2 -> assertThat(arg2).isReference().hasName("o_array['xs']")
        );
    }

    private String getSwappedOperator(String op) {
        for (var comp : ComparisonExpression.Type.values()) {
            if (comp.getValue().equals(op)) {
                var swappedComp = ExpressionAnalyzer.SWAP_OPERATOR_TABLE.get(comp);
                if (swappedComp == null) {
                    return op;
                }
                return swappedComp.getValue();
            }
        }
        return op;
    }

    @Test
    public void test_operator_cast_on_reference_is_moved_to_cast_on_literal() {
        for (var op : Operators.COMPARISON_OPERATORS) {
            op = op.replace(Operator.PREFIX, "");
            if (op.equals(ComparisonExpression.Type.CONTAINED_WITHIN.getValue())) {
                assertThat(toQuery("addr " + op + " '192.168.0.1/24'")).isFunction("op_<<");
            } else {
                assertThat(toQuery("id " + op + " 1.0")).isFunction(
                    "op_" + op,
                    arg1 -> assertThat(arg1).isReference().hasName("id"),
                    arg2 -> assertThat(arg2).isFunction("_cast")
                );
                assertThat(toQuery("1.0 " + op + " id")).isFunction(
                    "op_" + getSwappedOperator(op),
                    arg1 -> assertThat(arg1).isReference().hasName("id"),
                    arg2 -> assertThat(arg2).isFunction("_cast")
                );
            }
        }
    }

    @Test
    public void test_operator_subscript_on_reference_cast_is_moved_to_literal_cast() {
        assertThat(toQuery("ts_array[1] = 1129224512000")).isFunction(
            "op_=",
            arg1 -> assertThat(arg1).isFunction("subscript"),
            arg2 -> assertThat(arg2).isFunction("_cast")
        );
    }

    @Test
    public void test_operator_cast_on_array_length_with_reference_is_moved_to_literal_cast() {
        assertThat(toQuery("array_length(y_array, 1) < 1.0")).isFunction(
            "op_<",
            arg1 -> assertThat(arg1).isFunction("array_length"),
            arg2 -> assertThat(arg2).isFunction("_cast")
        );
    }
}
