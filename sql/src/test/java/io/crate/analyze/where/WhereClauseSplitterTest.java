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

package io.crate.analyze.where;

import io.crate.metadata.*;
import io.crate.metadata.relation.AnalyzedRelation;
import io.crate.metadata.relation.JoinRelation;
import io.crate.metadata.relation.TableRelation;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;
import io.crate.testing.QueryPrinter;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Map;

import static io.crate.testing.TestingHelpers.*;
import static io.crate.testing.TestingHelpers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;

public class WhereClauseSplitterTest {

    private WhereClauseSplitter splitter = new WhereClauseSplitter();
    private TableRelation tableEmps = new TableRelation(TestingTableInfo.builder(
            new TableIdent("doc", "employees"), RowGranularity.DOC, new Routing())
            .add(ColumnIdent.fromPath("id"), DataTypes.INTEGER)
            .add(ColumnIdent.fromPath("dep_id"), DataTypes.INTEGER)
            .addPrimaryKey("id")
            .add("name", DataTypes.STRING, null)
            .build(), mock(PartitionResolver.class));
    private TableRelation tableDeps = new TableRelation(TestingTableInfo.builder(
            new TableIdent("doc", "deps"), RowGranularity.DOC, new Routing())
            .add(ColumnIdent.fromPath("id"), DataTypes.INTEGER)
            .addPrimaryKey("id")
            .add("name", DataTypes.STRING, null)
            .build(), mock(PartitionResolver.class));
    private JoinRelation joinRelation =
            new JoinRelation(JoinRelation.Type.CROSS_JOIN, tableEmps, tableDeps);

    private Reference ref(TableInfo relation, String name, DataType type) {
        return new Reference(
                new ReferenceInfo(
                new ReferenceIdent(relation.ident(), ColumnIdent.fromPath(name)),
                RowGranularity.DOC, type));
    }

    @Test
    public void testSplitMatchAll() throws Exception {
        Map<AnalyzedRelation, Symbol> queryMap = splitter.split(WhereClause.MATCH_ALL, tableEmps);
        assertThat(queryMap.get(tableEmps), is(nullValue()));
    }

    @Test
    public void testSplitNoMatch() throws Exception {
        Map<AnalyzedRelation, Symbol> queryMap = splitter.split(WhereClause.NO_MATCH, tableEmps);
        assertThat(queryMap.get(tableEmps), is(nullValue()));
    }

    @Test
    public void testSplitLiteral() throws Exception {
        WhereClause clause = new WhereClause(Literal.newLiteral(true));
        Map<AnalyzedRelation, Symbol> queryMap = splitter.split(clause, tableEmps);
        assertThat(queryMap.get(tableEmps), is(nullValue()));
    }

    @Test
    public void testSplitSimpleWhereSingleTable() throws Exception {
        WhereClause clause = new WhereClause(eq(ref(tableEmps.tableInfo(), "name", DataTypes.STRING), Literal.newLiteral("Ford")));
        Map<AnalyzedRelation, Symbol> queryMap = splitter.split(clause, tableEmps);
        Symbol splitted = queryMap.get(tableEmps);
        assertThat(splitted, is(notNullValue()));
        assertThat(splitted, isFunction("op_="));
    }

    @Test
    public void testSplitSimpleWhereCrossJoin() throws Exception {
        WhereClause clause = new WhereClause(
                eq(
                        ref(tableEmps.tableInfo(), "name", DataTypes.STRING),
                        Literal.newLiteral("Ford")
                )
        );
        Map<AnalyzedRelation, Symbol> queryMap = splitter.split(clause, joinRelation);
        Symbol splitted = queryMap.get(tableEmps);
        assertThat(splitted, is(notNullValue()));
        assertThat(splitted, isFunction("op_="));

        splitted = queryMap.get(tableDeps);
        assertThat(splitted, is(nullValue()));
    }

    @Test
    public void testSplitCrossJoinBothReferenced() throws Exception {
        WhereClause clause = new WhereClause(
                and(
                        eq(
                                ref(tableEmps.tableInfo(), "name", DataTypes.STRING),
                                Literal.newLiteral("Ford")
                        ),
                        eq(
                                ref(tableDeps.tableInfo(), "id", DataTypes.INTEGER),
                                Literal.newLiteral(1)
                        )
                )

        );
        Map<AnalyzedRelation, Symbol> queryMap = splitter.split(clause, joinRelation);
        Symbol splitted = queryMap.get(tableEmps);
        assertThat(splitted, is(notNullValue()));

        assertThat(QueryPrinter.print(splitted), is("op_and(true, op_=(employees.name, 'Ford'))"));

        splitted = queryMap.get(tableDeps);
        assertThat(splitted, is(notNullValue()));
        assertThat(QueryPrinter.print(splitted), is("op_and(op_=(deps.id, 1), true)"));

    }

    @Test
    public void testSplitCrossJoinBothReferencedNested() throws Exception {
        WhereClause clause = new WhereClause(
                and(
                        eq(
                                substr(
                                        ref(tableEmps.tableInfo(), "name", DataTypes.STRING),
                                        2
                                ),
                                Literal.newLiteral("rd")
                        ),
                        eq(
                                ref(tableDeps.tableInfo(), "id", DataTypes.INTEGER),
                                Literal.newLiteral(1)
                        )
                )

        );
        Map<AnalyzedRelation, Symbol> queryMap = splitter.split(clause, joinRelation);
        Symbol empsQuery = queryMap.get(tableEmps);
        assertThat(empsQuery, is(notNullValue()));

        assertThat(QueryPrinter.print(empsQuery), is("op_and(true, op_=(substr(employees.name, 2), 'rd'))"));

        Symbol depsQuery = queryMap.get(tableDeps);
        assertThat(QueryPrinter.print(depsQuery), is("op_and(op_=(deps.id, 1), true)"));
    }

    @Test
    public void testSplitCrossJoinAndOrNot() throws Exception {
        WhereClause clause = new WhereClause(
                and(
                        or(
                            eq(
                                substr(
                                    ref(tableEmps.tableInfo(), "name", DataTypes.STRING),
                                    2
                                ),
                                Literal.newLiteral("rd")
                            ),
                            not(
                                    eq(
                                            ref(tableDeps.tableInfo(), "id", DataTypes.INTEGER),
                                            Literal.newLiteral(1)
                                    )
                            )
                        ),
                        eq(
                                ref(tableEmps.tableInfo(), "id", DataTypes.INTEGER),
                                Literal.newLiteral(42)
                        )
                )

        );
        Map<AnalyzedRelation, Symbol> queryMap = splitter.split(clause, joinRelation);
        Symbol splitted = queryMap.get(tableEmps);
        assertThat(splitted, is(notNullValue()));
        assertThat(splitted.symbolType(), is(SymbolType.FUNCTION));

        assertThat(QueryPrinter.print(splitted), is("op_and(op_=(employees.id, 42), op_or(false, op_=(substr(employees.name, 2), 'rd')))"));

        splitted = queryMap.get(tableDeps);
        assertThat(splitted, is(notNullValue()));
        assertThat(QueryPrinter.print(splitted), is("op_and(true, op_or(op_not(op_=(deps.id, 1)), false))"));
    }
}
