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
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.core.collections.*;
import io.crate.operation.Input;
import io.crate.operation.PageConsumeListener;
import io.crate.operation.PageDownstream;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.operation.merge.IteratorPageDownstream;
import io.crate.operation.merge.PassThroughPagingIterator;
import io.crate.operation.projectors.ListenableRowReceiver;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.projectors.SimpleTopNProjector;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.RowCollectionBucket;
import io.crate.testing.RowSender;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.common.util.CollectionUtils;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.isA;
import static org.hamcrest.core.Is.is;

public class NestedLoopOperationTest extends CrateUnitTest {

    private static final Predicate<Row> ROW_FILTER_PREDICATE = new Predicate<Row>() {
        @Override
        public boolean apply(@Nullable Row input) {
            return input != null && input.get(0) == input.get(1);
        }
    };

    private Bucket executeNestedLoop(List<Row> leftRows, List<Row> rightRows) throws Exception {
        return executeNestedLoop(leftRows, rightRows, Predicates.<Row>alwaysTrue(), JoinType.CROSS, 0, 0);
    }

    private Bucket executeNestedLoop(List<Row> leftRows,
                                     List<Row> rightRows,
                                     Predicate<Row> filterPredicate,
                                     JoinType joinType,
                                     int leftRowSize,
                                     int rightRowSize) throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        final NestedLoopOperation nestedLoopOperation = new NestedLoopOperation(0, rowReceiver, filterPredicate,
            joinType, leftRowSize, rightRowSize);

        PageDownstream leftPageDownstream = pageDownstream(nestedLoopOperation.leftRowReceiver());
        PageDownstream rightPageDownstream = pageDownstream(nestedLoopOperation.rightRowReceiver());

        Thread t1 = sendRowsThreaded("left", leftPageDownstream, leftRows);
        Thread t2 = sendRowsThreaded("right", rightPageDownstream, rightRows);
        t1.join();
        t2.join();
        return rowReceiver.result();
    }

    private static NestedLoopOperation unfilteredNestedLoopOperation(int phaseId, RowReceiver rowReceiver) {
        return new NestedLoopOperation(phaseId, rowReceiver, Predicates.<Row>alwaysTrue(), JoinType.CROSS, 0, 0);
    }

    private PageDownstream pageDownstream(RowReceiver rowReceiver) {
        return new IteratorPageDownstream(
                    rowReceiver,
                    PassThroughPagingIterator.<Void, Row>repeatable(),
                    Optional.<Executor>absent()
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
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        final NestedLoopOperation nestedLoopOperation = unfilteredNestedLoopOperation(0, rowReceiver);

        final PageDownstream leftBucketMerger = pageDownstream(nestedLoopOperation.leftRowReceiver());
        final PageDownstream rightBucketMerger = pageDownstream(nestedLoopOperation.rightRowReceiver());

        setLastPage(leftBucketMerger, Buckets.of(new Row1(1)));

        Bucket bucket = new RowCollectionBucket(Arrays.<Row>asList(new Row1(10), new Row1(20)));
        setLastPage(rightBucketMerger, bucket);

        assertThat(Buckets.materialize(rowReceiver.result()).length, is(2));
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
    public void testNestedLoopWithPausingDownstream() throws Exception {
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withPauseAfter(1);
        NestedLoopOperation nl = unfilteredNestedLoopOperation(0, rowReceiver);

        RowSender leftSender = new RowSender(asRows(1, 2, 3), nl.leftRowReceiver(), MoreExecutors.directExecutor());
        RowSender rightSender = new RowSender(asRows(10, 20), nl.rightRowReceiver(), MoreExecutors.directExecutor());

        rightSender.run();
        assertThat(rightSender.numPauses(), is(1));

        leftSender.run();

        // regression test: NestedLoop used to ignore the pause from the downstream and then the rowSize was 6 instead of 1
        assertThat(rowReceiver.rows.size(), is(1));
    }


    @Test
    @Repeat(iterations = 5)
    public void testNestedNestedLoop() throws Exception {
        /**
         *
         * RS-A  RS-B
         *   |   /
         * parentNl   RS-C
         *     \       /
         *      \     /
         *     childNl
         *        |
         *     finalOutput
         */
        CollectingRowReceiver finalOutput = new CollectingRowReceiver();
        NestedLoopOperation childNl = unfilteredNestedLoopOperation(1, finalOutput);
        NestedLoopOperation parentNl = unfilteredNestedLoopOperation(0, childNl.leftRowReceiver());

        RowSender rsA = new RowSender(
                asRows(1, 2), parentNl.leftRowReceiver(), MoreExecutors.directExecutor());

        RowSender rsB = new RowSender(
                asRows(10, 20), parentNl.rightRowReceiver(), MoreExecutors.directExecutor());

        RowSender rsC = new RowSender(
                asRows(100, 200), childNl.rightRowReceiver(), MoreExecutors.directExecutor());

        ArrayList<Thread> threads = new ArrayList<>();

        Thread tA = new Thread(rsA);
        Thread tB = new Thread(rsB);
        Thread tC = new Thread(rsC);

        threads.add(tA);
        threads.add(tB);
        threads.add(tC);

        CollectionUtils.rotate(threads, randomInt());

        for (Thread thread : threads) {
            thread.start();
        }

        Bucket result = finalOutput.result();
        assertThat(result.size(), is(8));
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
        InputCollectExpression firstCol = new InputCollectExpression(0);
        InputCollectExpression secondCol = new InputCollectExpression(1);
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        SimpleTopNProjector topNProjector = new SimpleTopNProjector(
                Arrays.<Input<?>>asList(firstCol, secondCol),
                Arrays.asList(firstCol, secondCol),
                3,
                1
        );
        topNProjector.downstream(rowReceiver);
        NestedLoopOperation nestedLoopOperation = unfilteredNestedLoopOperation(0, topNProjector);

        PageDownstream leftBucketMerger = pageDownstream(nestedLoopOperation.leftRowReceiver());
        PageDownstream rightBucketMerger = pageDownstream(nestedLoopOperation.rightRowReceiver());
        Thread leftT = sendRowsThreaded("left", leftBucketMerger, asRows("green", "blue", "red"));
        Thread rightT = sendRowsThreaded("right", rightBucketMerger, asRows("small", "medium"));

        Bucket rows = rowReceiver.result();
        assertThat(TestingHelpers.printedTable(rows), is("" +
                "green| medium\n" +
                "blue| small\n" +
                "blue| medium\n"));

        leftT.join();
        rightT.join();
    }

    @Test
    public void testFailIsOnlyForwardedOnce() throws Exception {
        CollectingRowReceiver receiver = new CollectingRowReceiver();
        List<ListenableRowReceiver> listenableRowReceivers = getRandomLeftAndRightRowReceivers(receiver);

        listenableRowReceivers.get(0).fail(new IllegalStateException("dummy1"));
        listenableRowReceivers.get(1).fail(new IllegalStateException("dummy2"));
        assertThat(receiver.getNumFailOrFinishCalls(), is(1));

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("dummy2");
        receiver.result();
    }

    @Test
    public void testFailAndFinishResultsInFailure() throws Exception {
        CollectingRowReceiver receiver = new CollectingRowReceiver();
        List<ListenableRowReceiver> listenableRowReceivers = getRandomLeftAndRightRowReceivers(receiver);

        listenableRowReceivers.get(0).fail(new IllegalStateException("dummy1"));
        listenableRowReceivers.get(1).finish(RepeatHandle.UNSUPPORTED);
        assertThat(receiver.getNumFailOrFinishCalls(), is(1));

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("dummy1");
        receiver.result();
    }

    @Test
    public void testFutureIsTriggeredOnKill() throws Exception {
        CollectingRowReceiver receiver = new CollectingRowReceiver();
        List<ListenableRowReceiver> listenableRowReceivers = getRandomLeftAndRightRowReceivers(receiver);
        ListenableRowReceiver receiver1 = listenableRowReceivers.get(0);
        receiver1.kill(new InterruptedException());

        expectedException.expectCause(isA(InterruptedException.class));
        listenableRowReceivers.get(1).finishFuture().get(1, TimeUnit.SECONDS);
    }

    @Test
    public void testFinishOnPausedLeftDoesNotCauseDeadlocksIfRightGotKilled() throws Exception {
        CollectingRowReceiver receiver = new CollectingRowReceiver();
        final NestedLoopOperation nl = unfilteredNestedLoopOperation(0, receiver);

        // initiate RowSender so that the RowReceiver has an upstream that can receive pause
        new RowSender(Collections.<Row>emptyList(), nl.leftRowReceiver(), MoreExecutors.directExecutor());

        nl.leftRowReceiver().setNextRow(new Row1(10)); // causes left to get paused
        nl.rightRowReceiver().kill(new InterruptedException());

        // a rowReceiver should not receive fail/or finish if paused but it can happen, e.g. if a node is stopped
        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                nl.leftRowReceiver().finish(RepeatHandle.UNSUPPORTED);
                latch.countDown();
            }
        });
        t.start();

        // latch is used to make sure there is a timeout exception if the finish within the thread is stuck
        latch.await(1, TimeUnit.SECONDS);

        // join is used to make sure the are no "lingering thread" warnings from randomized-test
        t.join(5000);
    }

    @Test
    public void testNestedLoopOperationWithLeftOuterJoin() throws Exception {
        List<Row> leftRows = asRows(1, 2, 3, 4, 5);
        List<Row> rightRows = asRows(3, 5);
        Bucket rows = executeNestedLoop(leftRows, rightRows, ROW_FILTER_PREDICATE, JoinType.LEFT, 1, 1);
        assertThat(TestingHelpers.printedTable(rows), is("1| NULL\n" +
                                                         "2| NULL\n" +
                                                         "3| 3\n" +
                                                         "4| NULL\n" +
                                                         "5| 5\n"));
    }

    @Test
    public void testNestedLoopOperationWithRightOuterJoin() throws Exception {
        List<Row> leftRows = asRows(3, 5);
        List<Row> rightRows = asRows(1, 2, 3, 4, 5);
        Bucket rows = executeNestedLoop(leftRows, rightRows, ROW_FILTER_PREDICATE, JoinType.RIGHT, 1, 1);
        assertThat(TestingHelpers.printedTable(rows), is("3| 3\n" +
                                                         "5| 5\n" +
                                                         "NULL| 1\n" +
                                                         "NULL| 2\n" +
                                                         "NULL| 4\n"));
    }

    @Test
    public void testNestedLoopOperationWithFullOuterJoin() throws Exception {
        List<Row> leftRows = asRows(3, 5, 6, 7);
        List<Row> rightRows = asRows(1, 2, 3, 4, 5);
        Bucket rows = executeNestedLoop(leftRows, rightRows, ROW_FILTER_PREDICATE, JoinType.FULL, 1, 1);
        assertThat(TestingHelpers.printedTable(rows), is("3| 3\n" +
                                                         "5| 5\n" +
                                                         "6| NULL\n" +
                                                         "7| NULL\n" +
                                                         "NULL| 1\n" +
                                                         "NULL| 2\n" +
                                                         "NULL| 4\n"));
    }

    private static List<ListenableRowReceiver> getRandomLeftAndRightRowReceivers(CollectingRowReceiver receiver) {
        NestedLoopOperation nestedLoopOperation = unfilteredNestedLoopOperation(0, receiver);

        List<ListenableRowReceiver> listenableRowReceivers =
                Arrays.asList(nestedLoopOperation.rightRowReceiver(), nestedLoopOperation.leftRowReceiver());
        Collections.shuffle(listenableRowReceivers, getRandom());
        return listenableRowReceivers;
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
}
