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

package io.crate.operator.projectors;

import io.crate.metadata.*;
import io.crate.operator.operations.ImplementationSymbolVisitor;
import io.crate.planner.RowGranularity;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.StringLiteral;
import io.crate.planner.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.operator.aggregation.impl.AggregationImplModule;
import io.crate.operator.aggregation.impl.AverageAggregation;
import io.crate.operator.aggregation.impl.CountAggregation;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.symbol.Aggregation;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;


public class ProjectionToProjectorVisitorTest {

    private ProjectionToProjectorVisitor visitor;
    private FunctionIdent countIdent;
    private FunctionIdent avgIdent;

    @Before
    public void prepare() {
        ReferenceResolver referenceResolver = new GlobalReferenceResolver(new HashMap<ReferenceIdent, ReferenceImplementation>());
        Injector injector = new ModulesBuilder().add(new AggregationImplModule()).createInjector();
        Functions functions = injector.getInstance(Functions.class);
        ImplementationSymbolVisitor symbolvisitor = new ImplementationSymbolVisitor(referenceResolver, functions, RowGranularity.NODE);
        visitor = new ProjectionToProjectorVisitor(symbolvisitor);

        countIdent = new FunctionIdent(CountAggregation.NAME, Arrays.asList(DataType.STRING));
        avgIdent = new FunctionIdent(AverageAggregation.NAME, Arrays.asList(DataType.INTEGER));
    }


    @Test
    public void testSimpleTopNProjection() {
        TopNProjection projection = new TopNProjection(10, 2);
        projection.outputs(Arrays.<Symbol>asList(new StringLiteral("foo"), new InputColumn(0)));
        List<Projector> projectors = visitor.process(Arrays.<Projection>asList(projection));
        assertThat(projectors.size(), is(1));
        Projector projector = projectors.get(0);
        assertThat(projector, instanceOf(SimpleTopNProjector.class));

        projector.startProjection();
        int i;
        for (i = 0; i<20; i++) {
            if (!projector.setNextRow(42)) {
                break;
            }
        }
        assertThat(i, is(12));
        projector.finishProjection();
        Object[][] rows = projector.getRows();
        assertThat(rows.length, is(10));
        assertThat((String)rows[0][0], is("foo"));
        assertThat((Integer)rows[0][1], is(42));
    }

    @Test
    public void testSortingTopNProjection() {
        TopNProjection projection = new TopNProjection(10, 0,
                Arrays.<Symbol>asList(new InputColumn(0), new InputColumn(1)), new boolean[]{false, false});

        projection.outputs(Arrays.<Symbol>asList(new StringLiteral("foo"), new InputColumn(0), new InputColumn(1)));
        List<Projector> projectors = visitor.process(Arrays.<Projection>asList(projection));
        assertThat(projectors.size(), is(1));
        Projector projector = projectors.get(0);
        assertThat(projector, instanceOf(SortingTopNProjector.class));

        projector.startProjection();
        int i;
        for (i = 20; i>0; i--) {
            if (!projector.setNextRow(i%4, i)) {
                break;
            }
        }
        assertThat(i, is(0));
        projector.finishProjection();
        Object[][] rows = projector.getRows();
        assertThat(rows.length, is(10));
        assertThat(rows[0].length, is(3));

        Object[] formerRow = null;
        for (Object[] row : rows) {
            if (formerRow == null) {
                formerRow = row;
                continue;
            }
            assertThat((String)row[0], is("foo"));
            assertThat((Integer)formerRow[1], lessThanOrEqualTo((Integer)row[1]));
            if (formerRow[1].equals(row[1]))  {
                assertThat((Integer)formerRow[2], lessThanOrEqualTo((Integer)row[2]));
            }
        }


    }

    @Test
    public void testGroupProjector() {
        //         in(0)  in(1)      in(0),      in(2)
        // select  race, avg(age), count(race), gender  ... group by race, gender
        GroupProjection projection = new GroupProjection();
        projection.keys(Arrays.<Symbol>asList(new InputColumn(0), new InputColumn(2)));
        projection.values(Arrays.asList(
                new Aggregation(avgIdent, Arrays.<Symbol>asList(new InputColumn(1)), Aggregation.Step.ITER, Aggregation.Step.FINAL),
                new Aggregation(countIdent, Arrays.<Symbol>asList(new InputColumn(0)), Aggregation.Step.ITER, Aggregation.Step.FINAL)
        ));

        Projector projector = visitor.process(projection, null);
        assertThat(projector, instanceOf(GroupingProjector.class));

        projector.startProjection();
        projector.setNextRow("human", 34, "male");
        projector.setNextRow("human", 22, "female");
        projector.setNextRow("vogon", 40, "male");
        projector.setNextRow("vogon", 48, "male");
        projector.setNextRow("human", 34, "male");
        projector.finishProjection();

        Object[][] rows = projector.getRows();
        assertThat(rows.length, is(3));
        assertThat((String)rows[0][0], is("human"));
        assertThat((String)rows[0][1], is("female"));
        assertThat((Double)rows[0][2], is(22.0));
        assertThat((Long)rows[0][3], is(1L));

        assertThat((String)rows[1][0], is("human"));
        assertThat((String)rows[1][1], is("male"));
        assertThat((Double)rows[1][2], is(34.0));
        assertThat((Long)rows[1][3], is(2L));

        assertThat((String)rows[2][0], is("vogon"));
        assertThat((String)rows[2][1], is("male"));
        assertThat((Double)rows[2][2], is(44.0));
        assertThat((Long)rows[2][3], is(2L));
    }
}
