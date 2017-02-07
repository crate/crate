/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.merge;

import com.google.common.util.concurrent.MoreExecutors;
import io.crate.data.Row;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.RowGenerator;
import io.crate.testing.TestingHelpers;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;

public class PagingIteratorEmitterTest extends CrateUnitTest {

    @Test
    public void testPauseWorksCorrectly() throws Exception {
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withPauseAfter(2);
        PassThroughPagingIterator<Integer, Row> pagingIterator = PassThroughPagingIterator.oneShot();
        PagingIteratorEmitter<Integer> emitter = new PagingIteratorEmitter<>(
            pagingIterator,
            rowReceiver,
            exhausted -> false,
            t -> {
            },
            MoreExecutors.directExecutor()
        );

        pagingIterator.merge(Arrays.asList(
            new KeyIterable<>(0, RowGenerator.range(0, 10)),
            new KeyIterable<>(1, RowGenerator.range(4, 8))
        ));

        emitter.consumeItAndFetchMore();

        assertThat(rowReceiver.rows.size(), is(2));
        rowReceiver.resumeUpstream(false);
        assertThat(rowReceiver.result().size(), is(14));
    }

    @Test
    public void testFailingDownstreamTriggersCloseCallback() throws Exception {
        CompletableFuture<Throwable> failFuture = new CompletableFuture<>();
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withFailure();
        PassThroughPagingIterator<Integer, Row> pagingIterator = PassThroughPagingIterator.oneShot();
        PagingIteratorEmitter<Integer> emitter = new PagingIteratorEmitter<>(
            pagingIterator,
            rowReceiver,
            exhausted -> false,
            failFuture::completeExceptionally,
            MoreExecutors.directExecutor()
        );
        pagingIterator.merge(Collections.singletonList(new KeyIterable<>(0, RowGenerator.range(0, 3))));
        emitter.consumeItAndFetchMore();

        expectedException.expectCause(instanceOf(IllegalStateException.class));
        expectedException.expectMessage("dummy");
        failFuture.get(10, TimeUnit.SECONDS);
    }

    @Test
    public void testStopFromDownstreamTriggersCallbackAndFinish() throws Exception {
        CompletableFuture<Throwable> callback = new CompletableFuture<>();
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withLimit(2);
        PassThroughPagingIterator<Integer, Row> pagingIterator = PassThroughPagingIterator.oneShot();
        PagingIteratorEmitter<Integer> emitter = new PagingIteratorEmitter<>(
            pagingIterator,
            rowReceiver,
            exhausted -> false,
            callback::complete,
            MoreExecutors.directExecutor()
        );
        pagingIterator.merge(Collections.singletonList(new KeyIterable<>(0, RowGenerator.range(1, 5))));
        emitter.consumeItAndFetchMore();

        assertThat(TestingHelpers.printedTable(rowReceiver.result()),
            is("1\n" +
               "2\n"));

        // must not timeout
        assertThat(callback.get(10, TimeUnit.SECONDS), nullValue());
    }

    @Test
    public void testRepeatIsSupported() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        PassThroughPagingIterator<Integer, Row> pagingIterator = PassThroughPagingIterator.repeatable();
        PagingIteratorEmitter<Integer> emitter = new PagingIteratorEmitter<>(
            pagingIterator,
            rowReceiver,
            exhausted -> false,
            t -> {},
            MoreExecutors.directExecutor()
        );
        pagingIterator.merge(Collections.singletonList(new KeyIterable<>(0, RowGenerator.range(1, 3))));
        emitter.consumeItAndFetchMore();

        rowReceiver.repeatUpstream();
        assertThat(rowReceiver.getNumFailOrFinishCalls(), is(2));
        assertThat(TestingHelpers.printedTable(rowReceiver.result()),
            is("1\n" +
               "2\n" +
               "1\n" +
               "2\n"));
    }
}
