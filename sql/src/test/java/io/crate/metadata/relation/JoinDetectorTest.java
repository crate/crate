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

package io.crate.metadata.relation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.where.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.scalar.arithmetic.AddFunction;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Literal;
import io.crate.types.DataTypes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class JoinDetectorTest {

    static final Routing shardRouting = new Routing(ImmutableMap.<String, Map<String, Set<Integer>>>builder()
            .put("nodeOne", ImmutableMap.<String, Set<Integer>>of("t1", ImmutableSet.of(1, 2)))
            .put("nodeTow", ImmutableMap.<String, Set<Integer>>of("t1", ImmutableSet.of(3, 4)))
            .build());

    static final TableInfo A = TestingTableInfo.builder(
            new TableIdent(null, "a"), RowGranularity.DOC, shardRouting).build();
    static final TableInfo B = TestingTableInfo.builder(
            new TableIdent(null, "b"), RowGranularity.DOC, shardRouting).build();
    static final TableInfo C = TestingTableInfo.builder(
            new TableIdent(null, "c"), RowGranularity.DOC, shardRouting).build();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testNoQueryTwoTables() throws Exception {
        // select * from a, b -> cross_join(a, b)
        List<AnalyzedRelation> relations = ImmutableList.<AnalyzedRelation>of(A, B);
        AnalyzedRelation relation = JoinDetector.buildRelation(relations, WhereClause.MATCH_ALL);
        assertThat(relation, instanceOf(JoinRelation.class));
    }

    @Test
    public void testNoQueryThreeTables() throws Exception {
        // select * from a, b, c ->  cross_join(cross_join(a, b), c)
        List<AnalyzedRelation> relations = ImmutableList.<AnalyzedRelation>of(A, B, C);
        AnalyzedRelation relation = JoinDetector.buildRelation(relations, WhereClause.MATCH_ALL);
        assertThat(relation, instanceOf(JoinRelation.class));

        JoinRelation rootJoin = (JoinRelation) relation;
        assertThat(rootJoin.left(), instanceOf(JoinRelation.class));
        assertThat(rootJoin.right(), instanceOf(TableInfo.class));
    }

    @Test
    public void testWhereAIdEqLiteralTwoTables() throws Exception {
        // select * from a,b where a.id = 1 -> cross join

        List<AnalyzedRelation> relations = ImmutableList.<AnalyzedRelation>of(A, B);
        WhereClause whereClause = new WhereClause(eq(
                        createReference("a", new ColumnIdent("id"), DataTypes.LONG),
                        Literal.newLiteral(10L)));

        AnalyzedRelation relation = JoinDetector.buildRelation(relations, whereClause);
        assertThat(relation, instanceOf(JoinRelation.class));
        assertThat(((JoinRelation) relation).type(), is(JoinRelation.Type.CROSS_JOIN));
    }

    @Test
    public void testWhereAIdEqBIdTwoTables() throws Exception {
        // select * from a, b where a.id = b.id

        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Detected join criteria in the WHERE clause but only cross joins are supported");

        List<AnalyzedRelation> relations = ImmutableList.<AnalyzedRelation>of(A, B);
        WhereClause whereClause = new WhereClause(eq(
                createReference("a", new ColumnIdent("id"), DataTypes.LONG),
                createReference("b", new ColumnIdent("id"), DataTypes.LONG)));

        JoinDetector.buildRelation(relations, whereClause);
    }

    @Test
    public void testWhereOneFunctionContainsTwoTablesEqLiteral() throws Exception {
        // select * from a, b where a.id + b.id = 2
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Detected join criteria in the WHERE clause but only cross joins are supported");

        List<AnalyzedRelation> relations = ImmutableList.<AnalyzedRelation>of(A, B);
        WhereClause whereClause = new WhereClause(
                eq(
                        createFunction(AddFunction.NAME, DataTypes.LONG,
                                createReference("a", new ColumnIdent("id"), DataTypes.LONG),
                                createReference("b", new ColumnIdent("id"), DataTypes.LONG)
                        ),
                        Literal.newLiteral(2L)
                )
        );
        JoinDetector.buildRelation(relations, whereClause);
    }

    @Test
    public void testWhereFunctionWithTableAEqFunctionWithTableB() throws Exception {
        // select * from a, b where substr(A.name, 1, 1) = substr(B.name, 1, 1)
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Detected join criteria in the WHERE clause but only cross joins are supported");

        List<AnalyzedRelation> relations = ImmutableList.<AnalyzedRelation>of(A, B);
        WhereClause whereClause = new WhereClause(
                eq(substr(createReference("a", new ColumnIdent("name"), DataTypes.STRING), 2),
                    substr(createReference("b", new ColumnIdent("name"), DataTypes.STRING), 2)));
        JoinDetector.buildRelation(relations, whereClause);
    }
}