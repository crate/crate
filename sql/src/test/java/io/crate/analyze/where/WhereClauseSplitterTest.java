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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.relation.AnalyzedRelation;
import io.crate.analyze.relation.CrossJoinRelation;
import io.crate.metadata.*;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.operator.AndOperator;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.operator.OrOperator;
import io.crate.operation.predicate.NotPredicate;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.operation.scalar.SubstrFunction;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import io.crate.testing.QueryPrinter;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class WhereClauseSplitterTest {

    private WhereClauseSplitter splitter = new WhereClauseSplitter();
    private TableInfo tableInfoEmps = TestingTableInfo.builder(
            new TableIdent("doc", "employees"), RowGranularity.DOC, new Routing())
            .add(ColumnIdent.fromPath("id"), DataTypes.INTEGER)
            .add(ColumnIdent.fromPath("dep_id"), DataTypes.INTEGER)
            .addPrimaryKey("id")
            .add("name", DataTypes.STRING, null)
            .build();
    private TableInfo tableInfoDeps = TestingTableInfo.builder(
            new TableIdent("doc", "deps"), RowGranularity.DOC, new Routing())
            .add(ColumnIdent.fromPath("id"), DataTypes.INTEGER)
            .addPrimaryKey("id")
            .add("name", DataTypes.STRING, null)
            .build();
    private CrossJoinRelation joinRelation = new CrossJoinRelation(tableInfoEmps, tableInfoDeps);
    private Functions functions;

    @Before
    public void setUp() throws Exception {
        Injector injector = new ModulesBuilder()
                .add(new OperatorModule(), new ScalarFunctionModule())
                .createInjector();
        functions = injector.getInstance(Functions.class);

    }

    private Reference ref(TableInfo relation, String name, DataType type) {
        return new Reference(
                new ReferenceInfo(
                new ReferenceIdent(relation.ident(), ColumnIdent.fromPath(name)),
                RowGranularity.DOC, type));
    }

    private Function eq(DataTypeSymbol left, DataTypeSymbol right) {
        FunctionInfo info = functions.get(
                new FunctionIdent(
                        EqOperator.NAME,
                        ImmutableList.of(left.valueType(), right.valueType()))).info();
        return new Function(info, Arrays.<Symbol>asList(left, right));
    }

    private Function and(@Nullable Symbol left, @Nullable Symbol right) {
        return new Function(AndOperator.INFO,
                Arrays.asList(left, right)
        );
    }

    private Function or(@Nullable Symbol left, @Nullable Symbol right) {
        return new Function(OrOperator.INFO, Arrays.asList(left, right));
    }

    private Function not(@Nullable Symbol negated) {
        return new Function(NotPredicate.INFO, Arrays.asList(negated));
    }

    private Function substr(Symbol str, int n) {
        FunctionInfo info = functions.get(
                new FunctionIdent(SubstrFunction.NAME,
                        Arrays.<DataType>asList(DataTypes.STRING, DataTypes.INTEGER))).info();
        return new Function(info, Arrays.asList(str, Literal.newLiteral(n)));
    }

    @Test
    public void testSplitMatchAll() throws Exception {
        Map<AnalyzedRelation, Symbol> queryMap = splitter.split(WhereClause.MATCH_ALL, tableInfoEmps);
        assertThat(queryMap.get(tableInfoEmps), is(nullValue()));
    }

    @Test
    public void testSplitNoMatch() throws Exception {
        Map<AnalyzedRelation, Symbol> queryMap = splitter.split(WhereClause.NO_MATCH, tableInfoEmps);
        assertThat(queryMap.get(tableInfoEmps), is(nullValue()));
    }

    @Test
    public void testSplitLiteral() throws Exception {
        WhereClause clause = new WhereClause(Literal.newLiteral(true));
        Map<AnalyzedRelation, Symbol> queryMap = splitter.split(clause, tableInfoEmps);
        assertThat(queryMap.get(tableInfoEmps), is(nullValue()));
    }

    @Test
    public void testSplitSimpleWhereSingleTable() throws Exception {
        WhereClause clause = new WhereClause(eq(ref(tableInfoEmps, "name", DataTypes.STRING), Literal.newLiteral("Ford")));
        Map<AnalyzedRelation, Symbol> queryMap = splitter.split(clause, tableInfoEmps);
        Symbol splitted = queryMap.get(tableInfoEmps);
        assertThat(splitted, is(notNullValue()));
        assertThat(splitted.symbolType(), is(SymbolType.FUNCTION));
        Function splittedQuery = (Function)splitted;
        assertThat(splittedQuery.info().ident().name(), is("op_="));
    }

    @Test
    public void testSplitSimpleWhereCrossJoin() throws Exception {
        WhereClause clause = new WhereClause(
                eq(
                        ref(tableInfoEmps, "name", DataTypes.STRING),
                        Literal.newLiteral("Ford")
                )
        );
        Map<AnalyzedRelation, Symbol> queryMap = splitter.split(clause, joinRelation);
        Symbol splitted = queryMap.get(tableInfoEmps);
        assertThat(splitted, is(notNullValue()));
        assertThat(splitted.symbolType(), is(SymbolType.FUNCTION));

        Function empsQuery = (Function)splitted;
        assertThat(empsQuery.info().ident().name(), is("op_="));

        splitted = queryMap.get(tableInfoDeps);
        assertThat(splitted, is(nullValue()));
    }

    @Test
    public void testSplitCrossJoinBothReferenced() throws Exception {
        WhereClause clause = new WhereClause(
                and(
                        eq(
                                ref(tableInfoEmps, "name", DataTypes.STRING),
                                Literal.newLiteral("Ford")
                        ),
                        eq(
                                ref(tableInfoDeps, "id", DataTypes.INTEGER),
                                Literal.newLiteral(1)
                        )
                )

        );
        Map<AnalyzedRelation, Symbol> queryMap = splitter.split(clause, joinRelation);
        Symbol splitted = queryMap.get(tableInfoEmps);
        assertThat(splitted, is(notNullValue()));
        assertThat(splitted.symbolType(), is(SymbolType.FUNCTION));

        Function empsQuery = (Function)splitted;
        assertThat(empsQuery.info().ident().name(), is("op_="));
        assertThat(empsQuery.arguments().get(0), TestingHelpers.isReference("name"));

        splitted = queryMap.get(tableInfoDeps);
        assertThat(splitted, is(notNullValue()));
        Function depsQuery = (Function)splitted;
        assertThat(depsQuery.info().ident().name(), is("op_="));
        assertThat(depsQuery.arguments().get(0), TestingHelpers.isReference("id"));
    }

    @Test
    public void testSplitCrossJoinBothReferencedNested() throws Exception {
        WhereClause clause = new WhereClause(
                and(
                        eq(
                                substr(
                                        ref(tableInfoEmps, "name", DataTypes.STRING),
                                        2
                                ),
                                Literal.newLiteral("rd")
                        ),
                        eq(
                                ref(tableInfoDeps, "id", DataTypes.INTEGER),
                                Literal.newLiteral(1)
                        )
                )

        );
        Map<AnalyzedRelation, Symbol> queryMap = splitter.split(clause, joinRelation);
        Symbol splitted = queryMap.get(tableInfoEmps);
        assertThat(splitted, is(notNullValue()));
        assertThat(splitted.symbolType(), is(SymbolType.FUNCTION));

        Function empsQuery = (Function) splitted;
        assertThat(empsQuery.info().ident().name(), is("op_and"));
        assertThat(empsQuery.arguments().get(0), TestingHelpers.isLiteral(true, DataTypes.BOOLEAN));
        assertThat(empsQuery.arguments().get(1), TestingHelpers.isFunction("op_="));

        splitted = queryMap.get(tableInfoDeps);
        assertThat(splitted, is(notNullValue()));
        Function depsQuery = (Function) splitted;
        assertThat(depsQuery.info().ident().name(), is("op_and"));
        assertThat(depsQuery.arguments().get(0), TestingHelpers.isFunction("op_="));
        assertThat(depsQuery.arguments().get(1), TestingHelpers.isLiteral(true, DataTypes.BOOLEAN));
    }

    @Test
    public void testSplitCrossJoinAndOrNot() throws Exception {
        WhereClause clause = new WhereClause(
                and(
                        or(
                            eq(
                                substr(
                                    ref(tableInfoEmps, "name", DataTypes.STRING),
                                    2
                                ),
                                Literal.newLiteral("rd")
                            ),
                            not(
                                    eq(
                                            ref(tableInfoDeps, "id", DataTypes.INTEGER),
                                            Literal.newLiteral(1)
                                    )
                            )
                        ),
                        eq(
                                ref(tableInfoEmps, "id", DataTypes.INTEGER),
                                Literal.newLiteral(42)
                        )
                )

        );
        Map<AnalyzedRelation, Symbol> queryMap = splitter.split(clause, joinRelation);
        Symbol splitted = queryMap.get(tableInfoEmps);
        assertThat(splitted, is(notNullValue()));
        assertThat(splitted.symbolType(), is(SymbolType.FUNCTION));

        assertThat(QueryPrinter.print(splitted), is("op_and(op_=(employees.id, 42), op_or(op_not(false), op_=(substr(employees.name, 2), 'rd')))"));

        splitted = queryMap.get(tableInfoDeps);
        assertThat(splitted, is(notNullValue()));
        assertThat(QueryPrinter.print(splitted), is("op_and(true, op_or(op_not(op_=(deps.id, 1)), false))"));
    }
}
