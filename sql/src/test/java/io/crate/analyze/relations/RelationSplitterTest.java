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

package io.crate.analyze.relations;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.*;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.metadata.*;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.operation.scalar.arithmetic.AddFunction;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Field;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RelationSplitterTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private EvaluatingNormalizer normalizer;
    private ExpressionAnalyzer expressionAnalyzer;
    private ExpressionAnalysisContext context;
    private TableRelation tr1;
    private TableRelation tr2;
    private TableInfo t1Info;
    private TableInfo t2Info;

    @Before
    public void setUp() throws Exception {
        Injector injector = new ModulesBuilder()
                .add(new ScalarFunctionModule())
                .add(new OperatorModule())
                .createInjector();

        Functions functions = injector.getInstance(Functions.class);
        ReferenceResolver referenceResolver = new ReferenceResolver() {

            @Override
            public ReferenceImplementation getImplementation(ReferenceIdent ident) {
                return null;
            }
        };
        normalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, referenceResolver);
        t1Info = tableInfoWith("t1", "x", "y");
        t2Info = tableInfoWith("t2", "x", "y", "z");
        tr1 = new TableRelation(t1Info);
        tr2 = new TableRelation(t2Info);
        Map<QualifiedName, AnalyzedRelation> sources = ImmutableMap.<QualifiedName, AnalyzedRelation>of(
                new QualifiedName("t1"), tr1,
                new QualifiedName("t2"), tr2
        );
        context = new ExpressionAnalysisContext();
        expressionAnalyzer = new ExpressionAnalyzer(
                new AnalysisMetaData(functions, mock(ReferenceInfos.class), referenceResolver),
                new ParameterContext(new Object[0], new Object[0][], null),
                new FullQualifedNameFieldProvider(sources)
        );
    }

    private TableInfo tableInfoWith(String tableName, String ... columns) {
        TableInfo tableInfo = mock(TableInfo.class);
        for (String column : columns) {
            when(tableInfo.getReferenceInfo(new ColumnIdent(column))).thenReturn(
                    new ReferenceInfo(new ReferenceIdent(new TableIdent("doc",tableName), column), RowGranularity.DOC, DataTypes.INTEGER));
        }
        return tableInfo;
    }

    private Symbol asSymbol(String expression) {
        return expressionAnalyzer.convert(SqlParser.createExpression(expression), context);
    }

    private QueriedTableRelation newSubRelation(TableRelation tr, QuerySpec querySpec) {
        RelationSplitter.SplitQuerySpecContext splitQuerySpecContext = RelationSplitter.splitQuerySpec(tr, querySpec);
        QueriedTableRelation queriedTable = new QueriedTable(tr, splitQuerySpecContext.outputNames(), splitQuerySpecContext.querySpec());
        RelationSplitter.replaceFields(queriedTable, querySpec, splitQuerySpecContext.querySpec());
        return queriedTable;
    }

    @Test
    public void testWhereClauseSplitWithMatchFunction() throws Exception {
        when(t1Info.getReferenceInfo(new ColumnIdent("name"))).thenReturn(
                new ReferenceInfo(new ReferenceIdent(new TableIdent("doc", "t1"), "name"), RowGranularity.DOC, DataTypes.STRING));

        Field t1x = new Field(tr1, new ColumnIdent("x"), DataTypes.INTEGER);
        Symbol symbol = asSymbol("match (name, 'search term')");
        QuerySpec querySpec = new QuerySpec()
                .outputs(Arrays.<Symbol>asList(t1x))
                .where(new WhereClause(symbol));
        QuerySpec splitQuerySpec = RelationSplitter.splitQuerySpec(tr1, querySpec).querySpec();

        assertThat(splitQuerySpec.where().query(), instanceOf(io.crate.planner.symbol.MatchPredicate.class));
        assertThat(querySpec.where().hasQuery(), is(false));
    }

    @Test
    public void testMatchWithColumnsFrom2Relations() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Must not use columns from more than 1 relation inside the MATCH predicate");

        when(t1Info.getReferenceInfo(new ColumnIdent("name"))).thenReturn(
                new ReferenceInfo(new ReferenceIdent(new TableIdent("doc", "t1"), "name"), RowGranularity.DOC, DataTypes.STRING));
        when(t2Info.getReferenceInfo(new ColumnIdent("foobar"))).thenReturn(
                new ReferenceInfo(new ReferenceIdent(new TableIdent("doc", "t2"), "foobar"), RowGranularity.DOC, DataTypes.STRING));

        Field t1x = new Field(tr1, new ColumnIdent("x"), DataTypes.INTEGER);
        Symbol symbol = expressionAnalyzer.convert(SqlParser.createExpression("match ((name, foobar), 'search term')"), context);
        QuerySpec querySpec = new QuerySpec()
                .outputs(Arrays.<Symbol>asList(t1x))
                .where(new WhereClause(symbol));

        RelationSplitter.splitQuerySpec(tr1, querySpec);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testQuerySpecSplit() throws Exception {
        /**
         * verify that:
         *      t1.x = 1 is fully pushed down
         *      t2.x = 3 is also fully pushed down
         *
         *      t1.y + t2.z = 3 is in "remaining query"
         *      and both t1.y, t2.z are added to the outputs of t1 / t2
         */
        Symbol query = asSymbol("t1.x = 1 and t2.x = 3 and t1.y + t2.z = 3");

        Field t1x = new Field(tr1, new ColumnIdent("x"), DataTypes.INTEGER);
        WhereClause whereClause = new WhereClause(query);

        QuerySpec querySpec = new QuerySpec()
                .outputs(Arrays.<Symbol>asList(t1x))
                .where(whereClause)
                .limit(30);

        QueriedTableRelation queriedTable1 = newSubRelation(tr1, querySpec);
        QueriedTableRelation queriedTable2 = newSubRelation(tr2, querySpec);

         // limit pushed down by default
        assertThat(queriedTable1.querySpec().limit(), is(30));
        assertThat(queriedTable2.querySpec().limit(), is(30));
        assertThat(querySpec.limit(), is(30));

        assertThat(queriedTable1.querySpec().outputs().size(), is(2));
        assertThat(queriedTable1.querySpec().outputs().get(0), isField("x"));
        assertThat(queriedTable1.querySpec().outputs().get(1), isField("y"));
        assertThat(queriedTable1.querySpec().where().query(), equalTo(asSymbol("t1.x = 1")));

        assertThat(queriedTable2.querySpec().outputs().size(), is(1));
        assertThat(queriedTable2.querySpec().outputs().get(0), isField("z"));
        assertThat(normalizer.normalize(queriedTable2.querySpec().where().query()), equalTo(asSymbol("t2.x = 3")));

        Function remainingQuery = (Function) normalizer.normalize(querySpec.where().query());
        assertThat(remainingQuery, isFunction(EqOperator.NAME));
        assertThat(remainingQuery.arguments().get(0), isFunction(AddFunction.NAME));
        assertThat(remainingQuery.arguments().get(1), isLiteral(3L));

        Function addFunction = (Function) remainingQuery.arguments().get(0);
        Symbol firstArg = addFunction.arguments().get(0);
        // must be a re-written field with qt1 as relation
        assertThat(firstArg, isField("y"));
        assertThat((QueriedTableRelation)((Field) firstArg).relation(), is(queriedTable1));

        Symbol secondArg = addFunction.arguments().get(1);
        assertThat(secondArg, isField("z"));
        assertThat((QueriedTableRelation) ((Field) secondArg).relation(), is(queriedTable2));
    }

    @Test
    public void testSplitQuerySpecOutputsOnly() throws Exception {
        TableRelation tr1 = new TableRelation(mock(TableInfo.class));
        TableRelation tr2 = new TableRelation(mock(TableInfo.class));

        QuerySpec querySpec = new QuerySpec();
        querySpec.outputs(Arrays.<Symbol>asList(
                new Field(tr1, new ColumnIdent("x"), DataTypes.STRING),
                new Field(tr2, new ColumnIdent("y"), DataTypes.STRING)
        ));
        QueriedTableRelation queriedTable = newSubRelation(tr1, querySpec);

        assertThat(queriedTable.querySpec().outputs().size(), is(1));
        assertThat(queriedTable.querySpec().outputs().get(0), isField("x"));

         // output of original querySpec has been rewritten, fields now point to the QueriedTable
        assertThat((QueriedTableRelation) ((Field) querySpec.outputs().get(0)).relation(), is(queriedTable));
        assertThat(querySpec.outputs().get(0), isField("x"));
    }

    @Test
    public void testFieldReplacingWorksForSelfJoin() throws Exception {
        TableInfo tableInfo = mock(TableInfo.class);
        TableRelation tr1 = new TableRelation(tableInfo);
        TableRelation tr2 = new TableRelation(tableInfo);

        QuerySpec querySpec = new QuerySpec();
        querySpec.outputs(Arrays.<Symbol>asList(
                new Field(tr1, new ColumnIdent("x"), DataTypes.STRING),
                new Field(tr2, new ColumnIdent("x"), DataTypes.STRING),
                new Field(tr2, new ColumnIdent("y"), DataTypes.STRING)
        ));
        QueriedTableRelation qt1 = newSubRelation(tr1, querySpec);

        assertThat(qt1.querySpec().outputs().size(), is(1));
        assertThat(qt1.querySpec().outputs().get(0), isField("x"));

        // output of original querySpec has been rewritten, fields now point to the QueriedTable
        assertThat((QueriedTableRelation) ((Field) querySpec.outputs().get(0)).relation(), is(qt1));
        assertThat(querySpec.outputs().get(0), isField("x"));

        QueriedTableRelation qt2 = newSubRelation(tr2, querySpec);
        assertThat(qt2.querySpec().outputs().size(), is(2));
        assertThat(qt2.querySpec().outputs().get(0), isField("x"));
        assertThat(qt2.querySpec().outputs().get(1), isField("y"));

        // output of original querySpec has been rewritten, fields now point to the QueriedTable
        assertThat((QueriedTableRelation) ((Field) querySpec.outputs().get(1)).relation(), is(qt2));
        assertThat(querySpec.outputs().get(1), isField("x"));
    }

    @Test
    public void testFieldReplacingWithSelfJoinAndFunctions() throws Exception {
        // emulate self join
        TableInfo tableInfo = mock(TableInfo.class);
        TableRelation tr1 = new TableRelation(tableInfo);
        TableRelation tr2 = new TableRelation(tableInfo);

        QuerySpec querySpec = new QuerySpec();
        querySpec.outputs(Arrays.asList(
                new Field(tr1, new ColumnIdent("x"), DataTypes.STRING),
                createFunction(AddFunction.NAME, DataTypes.INTEGER,
                        new Field(tr2, new ColumnIdent("x"), DataTypes.STRING), Literal.newLiteral(2))
        ));
        QueriedTableRelation qt1 = newSubRelation(tr1, querySpec);
        assertThat(qt1.querySpec().outputs().size(), is(1));
        assertThat(qt1.querySpec().outputs().get(0), isField("x"));

        QueriedTableRelation qt2 = newSubRelation(tr2, querySpec);
        assertThat(qt2.querySpec().outputs().size(), is(1));
        assertThat(qt2.querySpec().outputs().get(0), isFunction(AddFunction.NAME));


        // output of original querySpec has been rewritten, fields now point to the QueriedTable
        assertThat((QueriedTableRelation) ((Field) querySpec.outputs().get(0)).relation(), is(qt1));
        assertThat(querySpec.outputs().get(0), isField("x"));

        assertThat((QueriedTableRelation) ((Field) querySpec.outputs().get(1)).relation(), is(qt2));
        assertThat(querySpec.outputs().get(1), isField("add(string, integer)"));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testSplitOrderByThatIsPartiallyConsumed() throws Exception {
        // select t1.x from t1, t2 order by t1.x, t2.y + t1.z desc
        TableRelation tr1 = new TableRelation(mock(TableInfo.class));
        TableRelation tr2 = new TableRelation(mock(TableInfo.class));

        Field t1x = new Field(tr1, new ColumnIdent("x"), DataTypes.INTEGER);
        Field t1z = new Field(tr1, new ColumnIdent("z"), DataTypes.INTEGER);
        Field t2y = new Field(tr2, new ColumnIdent("y"), DataTypes.INTEGER);
        Function addT2y_T1z = createFunction(AddFunction.NAME, DataTypes.INTEGER, t2y, t1z);


        List<Symbol> orderBySymbols = Arrays.asList(t1x, addT2y_T1z);
        OrderBy orderBy = new OrderBy(orderBySymbols, new boolean[] { true, false }, new Boolean[] { null, null });

        QuerySpec querySpec = new QuerySpec()
                .outputs(Arrays.<Symbol>asList(t1x))
                .orderBy(orderBy);

        QueriedTableRelation qt1 = newSubRelation(tr1, querySpec);
        QueriedTableRelation qt2 = newSubRelation(tr2, querySpec);

        assertThat(qt1.querySpec().outputs().size(), is(2));
        assertThat(qt1.querySpec().outputs().get(0), isField("x"));
        assertThat(qt1.querySpec().outputs().get(1), isField("z"));
        assertThat(qt1.querySpec().orderBy(), Matchers.notNullValue());
        assertThat(qt1.querySpec().orderBy().orderBySymbols().size(), is(1));
        assertThat(qt1.querySpec().orderBy().orderBySymbols().get(0), isField("x"));
        assertThat(qt1.querySpec().orderBy().reverseFlags()[0], is(true));

        assertThat(querySpec.orderBy().orderBySymbols().size(), is(1));

        assertThat(qt2.querySpec().outputs().size(), is(1));
        assertThat(qt2.querySpec().outputs().get(0), isField("y"));
        assertThat(qt2.querySpec().orderBy(), nullValue());
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testSplitOrderByWith3RelationsButOutputsOnly2Relations() throws Exception {
        // select t1.x, t2.y from t1, t2, t3 order by t1.x, t2.y, t3.x
        TableRelation tr1 = new TableRelation(mock(TableInfo.class));
        TableRelation tr2 = new TableRelation(mock(TableInfo.class));
        TableRelation tr3 = new TableRelation(mock(TableInfo.class));

        Field t1x = new Field(tr1, new ColumnIdent("x"), DataTypes.INTEGER);
        Field t2y = new Field(tr2, new ColumnIdent("y"), DataTypes.LONG);
        Field t3x = new Field(tr3, new ColumnIdent("x"), DataTypes.SHORT);

        List<Symbol> orderBySymbols = Arrays.<Symbol>asList(t1x, t2y, t3x);
        OrderBy orderBy = new OrderBy(orderBySymbols, new boolean[] { false, false, false }, new Boolean[] { null, null, null });

        QuerySpec querySpec = new QuerySpec()
                .outputs(Arrays.<Symbol>asList(t1x, t2y))
                .orderBy(orderBy)
                .limit(20);

        QueriedTableRelation qt1 = newSubRelation(tr1, querySpec);
        QueriedTableRelation qt2 = newSubRelation(tr2, querySpec);
        QueriedTableRelation qt3 = newSubRelation(tr3, querySpec);

        // no join condition in order by.. limit can be pushed down
        assertThat(qt1.querySpec().limit(), is(20));
        assertThat(qt2.querySpec().limit(), is(20));
        assertThat(qt3.querySpec().limit(), is(20));

        assertThat(qt1.querySpec().orderBy().orderBySymbols().size(), is(1));
        assertThat(qt1.querySpec().orderBy().orderBySymbols().get(0), isField("x", DataTypes.INTEGER));

        assertThat(qt2.querySpec().orderBy().orderBySymbols().size(), is(1));
        assertThat(qt2.querySpec().orderBy().orderBySymbols().get(0), isField("y", DataTypes.LONG));

        assertThat(qt3.querySpec().orderBy().orderBySymbols().size(), is(1));
        assertThat(qt3.querySpec().orderBy().orderBySymbols().get(0), isField("x", DataTypes.SHORT));

        assertThat(querySpec.orderBy(), nullValue());
    }
}