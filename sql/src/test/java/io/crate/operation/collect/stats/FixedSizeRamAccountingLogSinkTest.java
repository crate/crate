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

package io.crate.operation.collect.stats;

import io.crate.breaker.RamAccountingContext;
import io.crate.breaker.SizeEstimator;
import io.crate.operation.reference.sys.job.ContextLog;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.*;

public class FixedSizeRamAccountingLogSinkTest extends CrateUnitTest {

    private static class NoopLog implements ContextLog {
        NoopLog() {
        }
        @Override
        public long started() {
            return 0;
        }
        @Override
        public long ended() {
            return 0;
        }
    }

    private static class  NoopLogEstimator extends SizeEstimator<NoopLog> {
        @Override
        public long estimateSize(@Nullable NoopLog value) {
            return 0L;
        }
    }

    private static final NoopLogEstimator NOOP_ESTIMATOR = new NoopLogEstimator();

    @Test
    public void testOffer() throws Exception {
        CircuitBreaker circuitBreaker =  mock(CircuitBreaker.class);
        // mocked CircuitBreaker has unlimited memory (⌐■_■)
        when(circuitBreaker.getLimit()).thenReturn(Long.MAX_VALUE);
        RamAccountingContext context = new RamAccountingContext("testRamAccountingContext", circuitBreaker);
        FixedSizeRamAccountingLogSink<NoopLog> logSink = new FixedSizeRamAccountingLogSink<>(context, 15_000, NOOP_ESTIMATOR::estimateSize);

        int THREADS = 50;
        final CountDownLatch latch = new CountDownLatch(THREADS);
        List<Thread> threads = new ArrayList<>(20);
        for (int i = 0; i < THREADS; i++) {
            Thread t = new Thread(() -> {
                for (int j = 0; j < 1000; j++) {
                    logSink.add(new NoopLog());
                }

                latch.countDown();
            });
            t.start();
            threads.add(t);
        }

        latch.await();
        assertThat(logSink.size(), is(15_000));
        for (Thread thread : threads) {
            thread.join();
        }
    }
}
