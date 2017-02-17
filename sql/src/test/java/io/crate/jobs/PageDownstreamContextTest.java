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

package io.crate.jobs;

import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.ArrayBucket;
import io.crate.data.Bucket;
import io.crate.data.CollectionBucket;
import io.crate.data.Row;
import io.crate.operation.PageResultListener;
import io.crate.operation.merge.PagingIterator;
import io.crate.operation.merge.PassThroughPagingIterator;
import io.crate.operation.merge.SortedPagingIterator;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.logging.Loggers;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class PageDownstreamContextTest extends CrateUnitTest {

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));

    private PageDownstreamContext getPageDownstreamContext(CollectingRowReceiver rowReceiver,
                                                           PagingIterator<Integer, Row> pagingIterator,
                                                           int numBuckets) {
        return new PageDownstreamContext(
            Loggers.getLogger(PageDownstreamContext.class),
            "n1",
            1,
            "dummy",
            rowReceiver,
            pagingIterator,
            new Streamer[0],
            RAM_ACCOUNTING_CONTEXT,
            numBuckets
        );
    }

    @Test
    public void testCantSetSameBucketTwiceWithoutReceivingFullPage() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();

        PageBucketReceiver ctx = getPageDownstreamContext(rowReceiver, PassThroughPagingIterator.oneShot(), 3);

        PageResultListener pageResultListener = mock(PageResultListener.class);
        Bucket bucket = new CollectionBucket(Collections.singletonList(new Object[] { "foo" }));
        ctx.setBucket(1, bucket, false, pageResultListener);
        ctx.setBucket(1, bucket, false, pageResultListener);

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Same bucket of a page set more than once. node=n1 method=setBucket phaseId=1 bucket=1");
        rowReceiver.result();
    }

    @Test
    public void testKillCallsDownstream() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        PageDownstreamContext ctx = getPageDownstreamContext(rowReceiver, PassThroughPagingIterator.oneShot(), 3);

        final AtomicReference<Throwable> throwable = new AtomicReference<>();
        ctx.completionFuture().whenComplete((r, t) -> {
            if (t != null) {
                assertTrue(throwable.compareAndSet(null, t));
            } else {
                fail("Expected exception");
            }
        });

        ctx.kill(null);
        assertThat(throwable.get(), instanceOf(InterruptedException.class));

        expectedException.expectCause(instanceOf(InterruptedException.class));
        rowReceiver.result();
    }

    @Test
    public void testPagingWithSortedPagingIterator() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        PageDownstreamContext ctx = getPageDownstreamContext(
            rowReceiver,
            new SortedPagingIterator<>(Comparator.comparingInt(r -> (int)r.get(0)), false),
            2
        );

        Bucket b1 = new ArrayBucket(new Object[][]{
            new Object[]{1},
            new Object[]{1},
        });
        Bucket b11 = new ArrayBucket(new Object[][]{
            new Object[]{2},
            new Object[]{2},
        });
        ctx.setBucket(0, b1, false, new PageResultListener() {
            @Override
            public void needMore(boolean needMore) {
                if (needMore) {
                    ctx.setBucket(0, b11, true, mock(PageResultListener.class));
                }
            }

            @Override
            public int buckedIdx() {
                return 0;
            }
        });
        Bucket b2 = new ArrayBucket(new Object[][] {
            new Object[] { 4 }
        });
        ctx.setBucket(1, b2, true, mock(PageResultListener.class));


        Bucket result = rowReceiver.result();
        assertThat(TestingHelpers.printedTable(result),
            is("1\n" +
               "1\n" +
               "2\n" +
               "2\n" +
               "4\n"));
    }
}
