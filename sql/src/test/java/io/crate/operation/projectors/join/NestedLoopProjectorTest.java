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

package io.crate.operation.projectors.join;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.Repeat;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.operation.projectors.CollectingProjector;
import io.crate.operation.projectors.Projector;
import io.crate.operation.projectors.SimpleTopNProjector;
import io.crate.testing.TestingHelpers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;

public class NestedLoopProjectorTest extends RandomizedTest {


    private Object[][] executeNestedLoop(List<Object[]> leftRows, List<Object[]> rightRows) throws Exception {
        NestedLoopProjector nlProjector = new NestedLoopProjector();
        final Projector leftProjector = nlProjector.leftProjector();
        final Projector rightProjector = nlProjector.rightProjector();

        CollectingProjector collectingProjector = new CollectingProjector();
        nlProjector.downstream(collectingProjector);
        nlProjector.startProjection();

        Thread t1 = sendRowsThreaded("left", leftProjector, leftRows);
        Thread t2 = sendRowsThreaded("right", rightProjector, rightRows);
        t1.join();
        t2.join();
        return collectingProjector.result().get();
    }

    private List<Object[]> asRows(Object ...rows) {
        List<Object[]> result = new ArrayList<>(rows.length);
        for (Object row : rows) {
            result.add(new Object[]{row});
        }
        return result;
    }

    @Test
    public void testLeftSideEmpty() throws Exception {
        Object[][] rows = executeNestedLoop(Arrays.<Object[]>asList(), asRows("small", "medium"));
        assertThat(rows.length, is(0));
    }

    @Test
    public void testRightSideIsEmpty() throws Exception {
        Object[][] rows = executeNestedLoop(asRows("small", "medium"), Arrays.<Object[]>asList());
        assertThat(rows.length, is(0));
    }

    @Test
    @Repeat (iterations = 5)
    public void testNestedLoopProjector() throws Exception {
        List<Object[]> leftRows = asRows("green", "blue", "red");
        List<Object[]> rightRows = asRows("small", "medium");

        Object[][] rows = executeNestedLoop(leftRows, rightRows);
        assertThat(TestingHelpers.printedTable(rows), is("" +
                "green| small\n" +
                "green| medium\n" +
                "blue| small\n" +
                "blue| medium\n" +
                "red| small\n" +
                "red| medium\n"));
    }

    @Test
    @Repeat (iterations = 5)
    public void testNestedLoopWithTopNDownstream() throws Exception {
        NestedLoopProjector nlProjector = new NestedLoopProjector();
        final Projector leftProjector = nlProjector.leftProjector();
        final Projector rightProjector = nlProjector.rightProjector();

        InputCollectExpression<Object> firstCol = new InputCollectExpression<>(0);
        InputCollectExpression<Object> secondCol = new InputCollectExpression<>(1);
        SimpleTopNProjector topNProjector = new SimpleTopNProjector(
                new Input[] { firstCol, secondCol },
                new CollectExpression[] { firstCol, secondCol },
                3,
                1
        );
        nlProjector.downstream(topNProjector);
        CollectingProjector collectingProjector = new CollectingProjector();
        topNProjector.downstream(collectingProjector);
        nlProjector.startProjection();

        Thread left = sendRowsThreaded("left", leftProjector, asRows("green", "blue", "red"));
        Thread right = sendRowsThreaded("right", rightProjector, asRows("small", "medium"));

        Object[][] rows = collectingProjector.result().get();
        assertThat(TestingHelpers.printedTable(rows), is("" +
                "green| medium\n" +
                "blue| small\n" +
                "blue| medium\n"));

        left.join();
        right.join();
    }

    private Thread sendRowsThreaded(String name, final Projector projector, final List<Object[]> rows) {
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    for (Object[] row : rows) {
                        projector.setNextRow(row);
                    }
                    projector.upstreamFinished();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        };
        t.setName(name);
        t.setDaemon(true);
        t.start();
        return t;
    }
}