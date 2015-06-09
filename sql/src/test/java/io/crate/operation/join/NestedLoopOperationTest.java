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

package io.crate.operation.join;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import io.crate.core.collections.*;
import io.crate.operation.*;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.operation.merge.IteratorPageDownstream;
import io.crate.operation.merge.PassThroughPagingIterator;
import io.crate.operation.projectors.SimpleTopNProjector;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingProjector;
import io.crate.testing.TestingHelpers;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;

public class NestedLoopOperationTest extends CrateUnitTest {

    private Bucket executeNestedLoop(List<Row> leftRows, List<Row> rightRows) throws Exception {
        final NestedLoopOperation nestedLoopOperation = new NestedLoopOperation();

        CollectingProjector collectingProjector = new CollectingProjector();
        nestedLoopOperation.downstream(collectingProjector);

        PageDownstream leftPageDownstream = pageDownstream(nestedLoopOperation);
        PageDownstream rightPageDownstream = pageDownstream(nestedLoopOperation);

        Thread t1 = sendRowsThreaded("left", leftPageDownstream, leftRows);
        Thread t2 = sendRowsThreaded("right", rightPageDownstream, rightRows);
        t1.join();
        t2.join();
        return collectingProjector.result().get(2, TimeUnit.SECONDS);
    }

    private PageDownstream pageDownstream(NestedLoopOperation nestedLoopOperation) {
        return new IteratorPageDownstream(
                    new NestedLoopRowDownstream(nestedLoopOperation),
                    new PassThroughPagingIterator<Row>(),
                    Optional.<Executor>absent(),
                    true
            );
    }

    private List<Row> asRows(Object ...rows) {
        List<Row> result = new ArrayList<>(rows.length);
        for (Object row : rows) {
            result.add(new Row1(row));
        }
        return result;
    }

    @Test
    public void testRightSideFinishesBeforeLeftSideStarts() throws Exception {
        final NestedLoopOperation nestedLoopOperation = new NestedLoopOperation();
        CollectingProjector collectingProjector = new CollectingProjector();
        nestedLoopOperation.downstream(collectingProjector);

        final PageDownstream leftBucketMerger = pageDownstream(nestedLoopOperation);
        final PageDownstream rightBucketMerger = pageDownstream(nestedLoopOperation);

        setLastPage(leftBucketMerger, Buckets.of(new Row1(1)));

        Bucket bucket = new RowCollectionBucket(Arrays.<Row>asList(new Row1(10), new Row1(20)));
        setLastPage(rightBucketMerger, bucket);

        assertThat(Buckets.materialize(collectingProjector.result().get()).length, is(2));
    }

    private void setLastPage(final PageDownstream pageDownstream, Bucket bucket) {
        pageDownstream.nextPage(new BucketPage(Futures.immediateFuture(bucket)), new PageConsumeListener() {
            @Override
            public void needMore() {
                pageDownstream.finish();
            }

            @Override
            public void finish() {
                pageDownstream.finish();
            }
        });
    }

    @Test
    public void testLeftSideEmpty() throws Exception {
        Bucket rows = executeNestedLoop(Collections.<Row>emptyList(), asRows("small", "medium"));
        assertThat(rows.size(), is(0));
    }

    @Test
    public void testRightSideIsEmpty() throws Exception {
        Bucket rows = executeNestedLoop(asRows("small", "medium"), Collections.<Row>emptyList());
        assertThat(rows.size(), is(0));
    }

    @Test
    @Repeat(iterations = 5)
    public void testNestedLoopOperation() throws Exception {
        List<Row> leftRows = asRows("green", "blue", "red");
        List<Row> rightRows = asRows("small", "medium");

        Bucket rows = executeNestedLoop(leftRows, rightRows);
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
        NestedLoopOperation nestedLoopOperation = new NestedLoopOperation();

        PageDownstream leftBucketMerger = pageDownstream(nestedLoopOperation);
        PageDownstream rightBucketMerger = pageDownstream(nestedLoopOperation);

        InputCollectExpression firstCol = new InputCollectExpression(0);
        InputCollectExpression secondCol = new InputCollectExpression(1);
        SimpleTopNProjector topNProjector = new SimpleTopNProjector(
                Arrays.<Input<?>>asList(firstCol, secondCol),
                new CollectExpression[] { firstCol, secondCol },
                3,
                1
        );
        nestedLoopOperation.downstream(topNProjector);
        CollectingProjector collectingProjector = new CollectingProjector();
        topNProjector.downstream(collectingProjector);

        Thread leftT = sendRowsThreaded("left", leftBucketMerger, asRows("green", "blue", "red"));
        Thread rightT = sendRowsThreaded("right", rightBucketMerger, asRows("small", "medium"));

        Bucket rows = collectingProjector.result().get(2, TimeUnit.SECONDS);
        assertThat(TestingHelpers.printedTable(rows), is("" +
                "green| medium\n" +
                "blue| small\n" +
                "blue| medium\n"));

        leftT.join();
        rightT.join();
    }

    private static class RowCollectionBucket implements Bucket {

        private Collection<Row> rows;

        public RowCollectionBucket(Collection<Row> rows) {
            this.rows = rows;
        }

        @Override
        public int size() {
            return rows.size();
        }

        @Override
        public Iterator<Row> iterator() {
            return rows.iterator();
        }
    }

    private Thread sendRowsThreaded(String name, final PageDownstream pageDownstream, final List<Row> rows) {
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    setLastPage(pageDownstream, new RowCollectionBucket(rows));
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

    private static class NestedLoopRowDownstream implements RowDownstream {
        private final NestedLoopOperation nestedLoopOperation;

        public NestedLoopRowDownstream(NestedLoopOperation nestedLoopOperation) {
            this.nestedLoopOperation = nestedLoopOperation;
        }

        @Override
        public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
            return nestedLoopOperation.registerUpstream(upstream);
        }
    }
}
