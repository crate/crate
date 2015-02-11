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

package io.crate.analyze;

import io.crate.analyze.relations.TableRelation;
import io.crate.metadata.*;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.operator.AndOperator;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.operation.scalar.arithmetic.AddFunction;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Field;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class QueriedTableTest {

    private EvaluatingNormalizer normalizer;

    @Before
    public void setUp() throws Exception {
        Injector injector = new ModulesBuilder()
                .add(new ScalarFunctionModule())
                .add(new OperatorModule())
                .createInjector();
        ReferenceResolver referenceResolver = new ReferenceResolver() {

            @Override
            public ReferenceImplementation getImplementation(ReferenceIdent ident) {
                return null;
            }
        };
        normalizer = new EvaluatingNormalizer(
                injector.getInstance(Functions.class),
                RowGranularity.CLUSTER,
                referenceResolver
        );
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testWhereClauseSplit() throws Exception {
        // select t1.x from t1, t2 where  t1.x = 1 and t2.x = 3 and t1.y + t2.z = 3

        /**
         * verify that:
         *      t1.x = 1 is fully pushed down
         *      t2.x = 3 is also fully pushed down
         *
         *      t1.y + t2.z = 3 is in "remaining query"
         *      and both t1.y, t2.z are added to the outputs of t1 / t2
         */

        TableRelation tr1 = new TableRelation(mock(TableInfo.class));
        TableRelation tr2 = new TableRelation(mock(TableInfo.class));
        Field t1x = new Field(tr1, new ColumnIdent("x"), DataTypes.INTEGER);
        Field t1y = new Field(tr1, new ColumnIdent("y"), DataTypes.INTEGER);
        Field t2x = new Field(tr2, new ColumnIdent("x"), DataTypes.INTEGER);
        Field t2z = new Field(tr2, new ColumnIdent("z"), DataTypes.INTEGER);

        Function t1xEq1 = createFunction(EqOperator.NAME, DataTypes.BOOLEAN, t1x, Literal.newLiteral(1));
        Function t2xEq3 = createFunction(EqOperator.NAME, DataTypes.BOOLEAN, t2x, Literal.newLiteral(3));
        Function t1yPlusT2z = createFunction(AddFunction.NAME, DataTypes.INTEGER, t1y, t2z);
        Function t1yPlusT2zEq3 = createFunction(EqOperator.NAME, DataTypes.BOOLEAN, t1yPlusT2z, Literal.newLiteral(3));

        Function t2xEq3AndT1yPlusT2zEq3 = createFunction(AndOperator.NAME, DataTypes.BOOLEAN, t2xEq3, t1yPlusT2zEq3);
        Function topAnd = createFunction(AndOperator.NAME, DataTypes.BOOLEAN, t1xEq1, t2xEq3AndT1yPlusT2zEq3);

        WhereClause whereClause = new WhereClause(topAnd);

        QuerySpec querySpec = new QuerySpec()
                .outputs(Arrays.<Symbol>asList(t1x))
                .where(whereClause);

        QueriedTable qt1 = QueriedTable.newSubRelation(new QualifiedName("t1"), tr1, querySpec);
        QueriedTable qt2 = QueriedTable.newSubRelation(new QualifiedName("t2"), tr2, querySpec);

        assertThat(qt1.querySpec().outputs().size(), is(2));
        assertThat(qt1.querySpec().outputs().get(0), isField("x"));
        assertThat(qt1.querySpec().outputs().get(1), isField("y"));
        assertThat(qt1.querySpec().where().query(), equalTo((Symbol) t1xEq1));

        assertThat(qt2.querySpec().outputs().size(), is(1));
        assertThat(qt2.querySpec().outputs().get(0), isField("z"));
        assertThat(qt2.querySpec().where().query(), equalTo((Symbol) t2xEq3));

        Function remainingQuery = (Function) normalizer.normalize(querySpec.where().query());
        assertThat(remainingQuery, isFunction(EqOperator.NAME));
        assertThat(remainingQuery.arguments().get(0), isFunction(AddFunction.NAME));
        assertThat(remainingQuery.arguments().get(1), isLiteral(3, DataTypes.INTEGER));

        Function addFunction = (Function) remainingQuery.arguments().get(0);
        Symbol firstArg = addFunction.arguments().get(0);
        // must be a re-written field with qt1 as relation
        assertThat(firstArg, isField("y"));
        assertTrue(((Field) firstArg).relation() == qt1);

        Symbol secondArg = addFunction.arguments().get(1);
        assertThat(secondArg, isField("z"));
        assertTrue(((Field) secondArg).relation() == qt2);
    }

    @Test
    public void testNewSubRelationOutputsOnly() throws Exception {
        TableRelation tr1 = new TableRelation(mock(TableInfo.class));
        TableRelation tr2 = new TableRelation(mock(TableInfo.class));

        QuerySpec querySpec = new QuerySpec();
        querySpec.outputs(Arrays.<Symbol>asList(
                new Field(tr1, new ColumnIdent("x"), DataTypes.STRING),
                new Field(tr2, new ColumnIdent("y"), DataTypes.STRING)
        ));
        QueriedTable queriedTable = QueriedTable.newSubRelation(new QualifiedName("t"), tr1, querySpec);
        assertThat(queriedTable.querySpec().outputs().size(), is(1));
        assertThat(queriedTable.querySpec().outputs().get(0), isField("x"));

         // output of original querySpec has been rewritten, fields now point to the QueriedTable
        assertTrue(((Field) querySpec.outputs().get(0)).relation() == queriedTable);
        assertThat(querySpec.outputs().get(0), isField("x"));
    }

    @Test
    public void testNewSubRelationFieldReplacingWorksForSelfJoin() throws Exception {
        TableInfo tableInfo = mock(TableInfo.class);
        TableRelation tr1 = new TableRelation(tableInfo);
        TableRelation tr2 = new TableRelation(tableInfo);

        QuerySpec querySpec = new QuerySpec();
        querySpec.outputs(Arrays.<Symbol>asList(
                new Field(tr1, new ColumnIdent("x"), DataTypes.STRING),
                new Field(tr2, new ColumnIdent("x"), DataTypes.STRING),
                new Field(tr2, new ColumnIdent("y"), DataTypes.STRING)
        ));
        QueriedTable qt1 = QueriedTable.newSubRelation(new QualifiedName("t"), tr1, querySpec);
        assertThat(qt1.querySpec().outputs().size(), is(1));
        assertThat(qt1.querySpec().outputs().get(0), isField("x"));

        // output of original querySpec has been rewritten, fields now point to the QueriedTable
        assertTrue(((Field) querySpec.outputs().get(0)).relation() == qt1);
        assertThat(querySpec.outputs().get(0), isField("x"));

        QueriedTable qt2 = QueriedTable.newSubRelation(new QualifiedName("t"), tr2, querySpec);
        assertThat(qt2.querySpec().outputs().size(), is(2));
        assertThat(qt2.querySpec().outputs().get(0), isField("x"));
        assertThat(qt2.querySpec().outputs().get(1), isField("y"));

        // output of original querySpec has been rewritten, fields now point to the QueriedTable
        assertTrue(((Field) querySpec.outputs().get(1)).relation() == qt2);
        assertThat(querySpec.outputs().get(1), isField("x"));
    }

    @Test
    public void testNewSubRelationFieldReplacingWithSelfJoinAndFunctions() throws Exception {
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
        QueriedTable qt1 = QueriedTable.newSubRelation(new QualifiedName("t"), tr1, querySpec);
        assertThat(qt1.querySpec().outputs().size(), is(1));
        assertThat(qt1.querySpec().outputs().get(0), isField("x"));

        QueriedTable qt2 = QueriedTable.newSubRelation(new QualifiedName("t"), tr2, querySpec);
        assertThat(qt2.querySpec().outputs().size(), is(1));
        assertThat(qt2.querySpec().outputs().get(0), isFunction(AddFunction.NAME));


        // output of original querySpec has been rewritten, fields now point to the QueriedTable
        assertTrue(((Field) querySpec.outputs().get(0)).relation() == qt1);
        assertThat(querySpec.outputs().get(0), isField("x"));

        assertTrue(((Field) querySpec.outputs().get(1)).relation() == qt2);
        assertThat(querySpec.outputs().get(1), isField("add(string, integer)"));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testNewSubRelationWithOrderByThatIsPartiallyConsumed() throws Exception {
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

        QueriedTable qt1 = QueriedTable.newSubRelation(new QualifiedName("t"), tr1, querySpec);
        QueriedTable qt2 = QueriedTable.newSubRelation(new QualifiedName("t"), tr2, querySpec);

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
    public void testNewSubRelationOrderBySplitWith3ButOnly2() throws Exception {
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
                .orderBy(orderBy);

        QueriedTable qt1 = QueriedTable.newSubRelation(new QualifiedName("t1"), tr1, querySpec);
        QueriedTable qt2 = QueriedTable.newSubRelation(new QualifiedName("t2"), tr2, querySpec);
        QueriedTable qt3 = QueriedTable.newSubRelation(new QualifiedName("t3"), tr3, querySpec);

        assertThat(qt1.querySpec().orderBy().orderBySymbols().size(), is(1));
        assertThat(qt1.querySpec().orderBy().orderBySymbols().get(0), isField("x", DataTypes.INTEGER));

        assertThat(qt2.querySpec().orderBy().orderBySymbols().size(), is(1));
        assertThat(qt2.querySpec().orderBy().orderBySymbols().get(0), isField("y", DataTypes.LONG));

        assertThat(qt3.querySpec().orderBy().orderBySymbols().size(), is(1));
        assertThat(qt3.querySpec().orderBy().orderBySymbols().get(0), isField("x", DataTypes.SHORT));

        assertThat(querySpec.orderBy(), nullValue());
    }
}