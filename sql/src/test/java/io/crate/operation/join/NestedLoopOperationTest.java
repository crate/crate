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
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.data.Bucket;
import io.crate.data.Buckets;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.Input;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.ResumeHandle;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.projectors.SimpleTopNProjector;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.RowSender;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.common.util.CollectionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static com.carrotsearch.randomizedtesting.RandomizedTest.getRandom;
import static io.crate.testing.RowGenerator.singleColRows;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.core.Is.is;
import static org.junit.matchers.JUnitMatchers.isThrowable;

public class NestedLoopOperationTest extends CrateUnitTest {

    private ExecutorService executorService;

    private static final Predicate<Row> COL0_EQ_COL1 = r -> r.get(0) == r.get(1);

    @Before
    public void setupExecutor() throws Exception {
        executorService = Executors.newFixedThreadPool(2);
    }

    @After
    public void shutdownExecutor() throws Exception {
        executorService.shutdown();
        executorService.awaitTermination(30, TimeUnit.SECONDS);
    }

    private Bucket executeNestedLoop(List<Row> leftRows, List<Row> rightRows) throws Exception {
        return executeNestedLoop(
            leftRows, rightRows, r -> true, JoinType.CROSS, 0, 0);
    }

    private Bucket executeNestedLoop(List<Row> leftRows,
                                     List<Row> rightRows,
                                     Predicate<Row> joinPredicate,
                                     JoinType joinType,
                                     int leftRowSize,
                                     int rightRowSize) throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        final NestedLoopOperation nestedLoopOperation = new NestedLoopOperation(
            0, rowReceiver, joinPredicate, joinType, leftRowSize, rightRowSize);

        RowSender rsLeft = new RowSender(leftRows, nestedLoopOperation.leftRowReceiver(), executorService);
        RowSender rsRight = new RowSender(rightRows, nestedLoopOperation.rightRowReceiver(), executorService);
        executorService.submit(rsLeft);
        executorService.submit(rsRight);
        return rowReceiver.result();
    }

    private static NestedLoopOperation unfilteredNestedLoopOperation(int phaseId, RowReceiver rowReceiver) {
        return new NestedLoopOperation(
            phaseId, rowReceiver, r -> true, JoinType.CROSS, 0, 0);
    }

    @Test
    public void testRightSideFinishesBeforeLeftSideStarts() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        final NestedLoopOperation nestedLoopOperation = unfilteredNestedLoopOperation(0, rowReceiver);

        RowSender rsLeft = new RowSender(singleColRows(1), nestedLoopOperation.leftRowReceiver(), executorService);
        RowSender rsRight = new RowSender(singleColRows(10, 20), nestedLoopOperation.rightRowReceiver(), executorService);

        rsRight.run();
        rsLeft.run();
        assertThat(Buckets.materialize(rowReceiver.result()).length, is(2));
    }

    @Test
    public void testLeftSideEmpty() throws Exception {
        Bucket rows = executeNestedLoop(Collections.<Row>emptyList(), singleColRows("small", "medium"));
        assertThat(rows.size(), is(0));
    }

    @Test
    public void testRightSideIsEmpty() throws Exception {
        Bucket rows = executeNestedLoop(singleColRows("small", "medium"), Collections.<Row>emptyList());
        assertThat(rows.size(), is(0));
    }

    @Test
    public void testNestedLoopWithPausingDownstream() throws Exception {
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withPauseAfter(1);
        NestedLoopOperation nl = unfilteredNestedLoopOperation(0, rowReceiver);

        RowSender leftSender = new RowSender(singleColRows(1, 2, 3), nl.leftRowReceiver(), MoreExecutors.directExecutor());
        RowSender rightSender = new RowSender(singleColRows(10, 20), nl.rightRowReceiver(), MoreExecutors.directExecutor());

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
            singleColRows(1, 2), parentNl.leftRowReceiver(), MoreExecutors.directExecutor());

        RowSender rsB = new RowSender(
            singleColRows(10, 20), parentNl.rightRowReceiver(), MoreExecutors.directExecutor());

        RowSender rsC = new RowSender(
            singleColRows(100, 200), childNl.rightRowReceiver(), MoreExecutors.directExecutor());

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
        List<Row> leftRows = singleColRows("green", "blue", "red");
        List<Row> rightRows = singleColRows("small", "medium");

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
    public void testNestedDoesStopOnceDownstreamStops() throws Exception {
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withLimit(1);
        final NestedLoopOperation op = new NestedLoopOperation(
            0, rowReceiver, r -> true, JoinType.INNER, 1, 1);

        // the left RR immediately pauses, since the op changes to right
        assertThat(op.leftRowReceiver().setNextRow(new Row1(1)), is(RowReceiver.Result.PAUSE));
        // the downstream stops immediately, therefore the STOP gets propagated
        assertThat(op.rightRowReceiver().setNextRow(new Row1(1)), is(RowReceiver.Result.STOP));

        op.leftRowReceiver().pauseProcessed(new ResumeHandle() {
            @Override
            public void resume(boolean async) {
                // once the left side gets resumed it must receive a STOP immediately
                assertThat(op.leftRowReceiver().setNextRow(new Row1(1)), is(RowReceiver.Result.STOP));
                op.leftRowReceiver().finish(RepeatHandle.UNSUPPORTED);
            }
        });
        op.rightRowReceiver().finish(RepeatHandle.UNSUPPORTED);
        assertThat(rowReceiver.getNumFailOrFinishCalls(), is(1));
    }

    @Test
    @Repeat(iterations = 5)
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
        NestedLoopOperation nl = unfilteredNestedLoopOperation(0, topNProjector);

        executorService.submit(
            new RowSender(singleColRows("green", "blue", "red"), nl.leftRowReceiver(), executorService));
        executorService.submit(
            new RowSender(singleColRows("small", "medium"), nl.rightRowReceiver(), executorService));

        Bucket rows = rowReceiver.result();
        assertThat(TestingHelpers.printedTable(rows), is("" +
                                                         "green| medium\n" +
                                                         "blue| small\n" +
                                                         "blue| medium\n"));
    }

    @Test
    public void testRightJoinLeftUpstreamFails() throws Exception {
        CollectingRowReceiver receiver = new CollectingRowReceiver();

        NestedLoopOperation nl = new NestedLoopOperation(1, receiver, COL0_EQ_COL1, JoinType.RIGHT, 1, 1);
        nl.leftRowReceiver().fail(new InterruptedException("Job killed"));
        RowSender.generateRowsInRangeAndEmit(0, 10, nl.rightRowReceiver());

        expectedException.expect(instanceOf(RuntimeException.class));
        receiver.result();
    }

    @Test
    public void testRightJoinRightUpstreamFails() throws Exception {
        CollectingRowReceiver receiver = new CollectingRowReceiver();
        NestedLoopOperation nl = new NestedLoopOperation(1, receiver, COL0_EQ_COL1, JoinType.RIGHT, 1, 1);

        nl.rightRowReceiver().fail(new InterruptedException("Job killed"));
        RowSender.generateRowsInRangeAndEmit(0, 10, nl.leftRowReceiver());

        expectedException.expect(instanceOf(RuntimeException.class));
        receiver.result();
    }

    @Test
    public void testRightJoinDownstreamFailure() throws Exception {
        CollectingRowReceiver receiver = CollectingRowReceiver.withFailure();
        NestedLoopOperation nl = new NestedLoopOperation(1, receiver, COL0_EQ_COL1, JoinType.RIGHT, 1, 1);

        RowSender.generateRowsInRangeAndEmit(0, 5, nl.leftRowReceiver());
        RowSender.generateRowsInRangeAndEmit(0, 5, nl.rightRowReceiver());

        expectedException.expect(instanceOf(IllegalStateException.class));
        receiver.result();
    }

    @Test
    public void testFailIsOnlyForwardedOnce() throws Exception {
        CollectingRowReceiver receiver = new CollectingRowReceiver();
        List<RowReceiver> listenableRowReceivers = getRandomLeftAndRightRowReceivers(receiver);

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
        List<RowReceiver> listenableRowReceivers = getRandomLeftAndRightRowReceivers(receiver);

        listenableRowReceivers.get(0).fail(new IllegalStateException("dummy1"));
        listenableRowReceivers.get(1).finish(RepeatHandle.UNSUPPORTED);
        assertThat(receiver.getNumFailOrFinishCalls(), is(1));

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("dummy1");
        receiver.result();
    }

    @Test
    public void testRightUpstreamFailureOnLeftFirstResultsInFailure() throws Exception {
        CollectingRowReceiver receiver = new CollectingRowReceiver();

        NestedLoopOperation nestedLoopOperation = unfilteredNestedLoopOperation(0, receiver);
        RowReceiver left = nestedLoopOperation.leftRowReceiver();
        RowReceiver right = nestedLoopOperation.rightRowReceiver();

        List<Row> leftRows = singleColRows(1, 2, 3);
        RowSender rsLeft = new RowSender(leftRows, left, executorService);
        RowSender rsRight = RowSender.withFailure(right, executorService); // fail immediately
        rsLeft.run();
        rsRight.run();

        try {
            receiver.result();
            fail("expecting to throw an exception");
        } catch (Throwable t) {
            assertThat(t, isThrowable(instanceOf(IllegalStateException.class)));
        }

        assertThat(receiver.getNumFailOrFinishCalls(), is(1));
    }

    @Test
    public void testLeftUpstreamFailureOnRightFirstResultsInFailure() throws Exception {
        CollectingRowReceiver receiver = new CollectingRowReceiver();

        NestedLoopOperation nestedLoopOperation = unfilteredNestedLoopOperation(0, receiver);
        RowReceiver left = nestedLoopOperation.leftRowReceiver();
        RowReceiver right = nestedLoopOperation.rightRowReceiver();

        List<Row> rightRows = singleColRows(1, 2, 3);
        RowSender rsLeft = RowSender.withFailure(left, executorService); // fail immediately
        RowSender rsRight = new RowSender(rightRows, right, executorService);
        rsRight.run();
        rsLeft.run();

        try {
            receiver.result();
            fail("expecting to throw an exception");
        } catch (Throwable t) {
            assertThat(t, isThrowable(instanceOf(IllegalStateException.class)));
        }

        assertThat(receiver.getNumFailOrFinishCalls(), is(1));
    }

    @Test
    public void testDownstreamFailOnLeftFirstResultsInFailure() throws Exception {
        CollectingRowReceiver receiver = CollectingRowReceiver.withFailure();

        NestedLoopOperation nestedLoopOperation = unfilteredNestedLoopOperation(0, receiver);
        RowReceiver left = nestedLoopOperation.leftRowReceiver();
        RowReceiver right = nestedLoopOperation.rightRowReceiver();

        List<Row> leftRows = singleColRows(1, 2);
        List<Row> rightRows = singleColRows(1, 2);
        RowSender rsLeft = new RowSender(leftRows, left, executorService);
        RowSender rsRight = new RowSender(rightRows, right, executorService);
        rsLeft.run();
        rsRight.run();

        try {
            receiver.result();
            fail("expecting to throw an exception");
        } catch (Throwable t) {
            assertThat(t, isThrowable(instanceOf(IllegalStateException.class)));
        }

        assertThat(receiver.getNumFailOrFinishCalls(), is(1));
    }

    @Test
    public void testDownstreamFailOnRightFirstResultsInFailure() throws Exception {
        CollectingRowReceiver receiver = CollectingRowReceiver.withFailure();

        NestedLoopOperation nestedLoopOperation = unfilteredNestedLoopOperation(0, receiver);
        RowReceiver left = nestedLoopOperation.leftRowReceiver();
        RowReceiver right = nestedLoopOperation.rightRowReceiver();

        List<Row> leftRows = singleColRows(1, 2);
        List<Row> rightRows = singleColRows(1, 2);
        RowSender rsLeft = new RowSender(leftRows, left, executorService);
        RowSender rsRight = new RowSender(rightRows, right, executorService);
        rsRight.run();
        rsLeft.run();

        try {
            receiver.result();
            fail("expecting to throw an exception");
        } catch (Throwable t) {
            assertThat(t, isThrowable(instanceOf(IllegalStateException.class)));
        }

        assertThat(receiver.getNumFailOrFinishCalls(), is(1));
    }

    @Test
    public void testDownstreamFailOnLeftJoinEmitResultsInFailure() throws Exception {
        CollectingRowReceiver receiver = CollectingRowReceiver.withFailure();

        NestedLoopOperation nestedLoopOperation = new NestedLoopOperation(
            0, receiver, COL0_EQ_COL1, JoinType.LEFT, 1, 1);
        RowReceiver left = nestedLoopOperation.leftRowReceiver();
        RowReceiver right = nestedLoopOperation.rightRowReceiver();

        List<Row> leftRows = singleColRows(1, 2);
        List<Row> rightRows = singleColRows(2, 3);
        RowSender rsLeft = new RowSender(leftRows, left, executorService);
        RowSender rsRight = new RowSender(rightRows, right, executorService);
        rsLeft.run();
        rsRight.run();

        try {
            receiver.result();
            fail("expecting to throw an exception");
        } catch (Throwable t) {
            assertThat(t, isThrowable(instanceOf(IllegalStateException.class)));
        }

        assertThat(receiver.getNumFailOrFinishCalls(), is(1));
    }

    @Test
    public void testFutureIsTriggeredOnKill() throws Exception {
        CollectingRowReceiver receiver = new CollectingRowReceiver();
        List<RowReceiver> listenableRowReceivers = getRandomLeftAndRightRowReceivers(receiver);
        RowReceiver receiver1 = listenableRowReceivers.get(0);
        receiver1.kill(new InterruptedException());

        expectedException.expectCause(isA(InterruptedException.class));
        listenableRowReceivers.get(1).completionFuture().get(1, TimeUnit.SECONDS);
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
        List<Row> leftRows = singleColRows(1, 2, 3, 4, 5);
        List<Row> rightRows = singleColRows(3, 5);
        Bucket rows = executeNestedLoop(
            leftRows, rightRows, COL0_EQ_COL1, JoinType.LEFT, 1, 1);
        assertThat(TestingHelpers.printedTable(rows), is("1| NULL\n" +
                                                         "2| NULL\n" +
                                                         "3| 3\n" +
                                                         "4| NULL\n" +
                                                         "5| 5\n"));
    }

    @Test
    public void testNestedLoopOperationWithRightOuterJoin() throws Exception {
        List<Row> leftRows = singleColRows(3, 5);
        List<Row> rightRows = singleColRows(1, 2, 3, 4, 5);
        Bucket rows = executeNestedLoop(
            leftRows, rightRows, COL0_EQ_COL1, JoinType.RIGHT, 1, 1);
        assertThat(TestingHelpers.printedTable(rows), is("3| 3\n" +
                                                         "5| 5\n" +
                                                         "NULL| 1\n" +
                                                         "NULL| 2\n" +
                                                         "NULL| 4\n"));
    }

    @Test
    public void testNestedLoopOperationWithFullOuterJoin() throws Exception {
        List<Row> leftRows = singleColRows(3, 5, 6, 7);
        List<Row> rightRows = singleColRows(1, 2, 3, 4, 5);
        Bucket rows = executeNestedLoop(
            leftRows, rightRows, COL0_EQ_COL1, JoinType.FULL, 1, 1);
        assertThat(TestingHelpers.printedTable(rows), is("3| 3\n" +
                                                         "5| 5\n" +
                                                         "6| NULL\n" +
                                                         "7| NULL\n" +
                                                         "NULL| 1\n" +
                                                         "NULL| 2\n" +
                                                         "NULL| 4\n"));
    }

    @Test
    public void testLeftJoinWithEmptyJoinedTable() throws Exception {
        List<Row> leftRows = singleColRows(1, 2, 3);
        List<Row> rightRows = Collections.emptyList();
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        NestedLoopOperation nl = new NestedLoopOperation(
            0, rowReceiver, COL0_EQ_COL1, JoinType.LEFT, 1, 1);

        executorService.submit(new RowSender(leftRows, nl.leftRowReceiver(), executorService));
        executorService.submit(new RowSender(rightRows, nl.rightRowReceiver(), executorService));

        assertThat(TestingHelpers.printedTable(rowReceiver.result()),
            is("1| NULL\n" +
               "2| NULL\n" +
               "3| NULL\n"));
    }

    @Test
    public void testRightJoinWithEmptyJoinedTable() throws Exception {
        List<Row> leftRows = Collections.emptyList();
        List<Row> rightRows = singleColRows(1, 2, 3);
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        NestedLoopOperation nl = new NestedLoopOperation(
            0, rowReceiver, COL0_EQ_COL1, JoinType.RIGHT, 1, 1);

        RowSender rsLeft = new RowSender(leftRows, nl.leftRowReceiver(), executorService);
        RowSender rsRight = new RowSender(rightRows, nl.rightRowReceiver(), executorService);
        executorService.submit(rsLeft);
        executorService.submit(rsRight );

        assertThat(TestingHelpers.printedTable(rowReceiver.result()),
            is("NULL| 1\n" +
               "NULL| 2\n" +
               "NULL| 3\n"));
    }

    private static List<RowReceiver> getRandomLeftAndRightRowReceivers(CollectingRowReceiver receiver) {
        NestedLoopOperation nestedLoopOperation = unfilteredNestedLoopOperation(0, receiver);

        List<RowReceiver> listenableRowReceivers =
            Arrays.asList(nestedLoopOperation.rightRowReceiver(), nestedLoopOperation.leftRowReceiver());
        Collections.shuffle(listenableRowReceivers, getRandom());
        return listenableRowReceivers;
    }
}
