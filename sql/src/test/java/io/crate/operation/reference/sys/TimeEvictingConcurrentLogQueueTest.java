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

package io.crate.operation.reference.sys;

import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class TimeEvictingConcurrentLogQueueTest extends CrateUnitTest {

    private static class TestLog implements Log {

        private final String message;

        private final long ended;

        public TestLog(String message, long ended) {
            this.message = message;
            this.ended = ended;
        }

        @Override
        public long started() {
            return 0;
        }

        @Override
        public long ended() {
            return ended;
        }
    }

    @Test
    public void testRemoveExpiredLogs() throws Exception {
        TimeEvictingConcurrentLogQueue queue = new TimeEvictingConcurrentLogQueue(TimeValue.timeValueSeconds(5));
        queue.offer(new TestLog("expired", 2000L));
        queue.offer(new TestLog("expired", 4000L));
        queue.offer(new TestLog("not expired yet", 7000L));
        queue.removeExpired(10000L);
        assertThat(queue.size(), is(1));
        assertThat(((TestLog)queue.poll()).message, is("not expired yet"));
    }

    @Test
    public void testStopCleaningUp() throws Exception {
        TimeEvictingConcurrentLogQueue queue = new TimeEvictingConcurrentLogQueue(TimeValue.timeValueSeconds(5));
        queue.offer(new TestLog("expired", 2000L));
        queue.offer(new TestLog("not expired", 8000L));
        queue.offer(new TestLog("expired but added late", 3000L));
        // The cleanup stops after the first not expired element
        queue.removeExpired(9000);
        assertThat(queue.size(), is(2));
        assertThat(((TestLog)queue.poll()).message, is("not expired"));
        assertThat(((TestLog)queue.poll()).message, is("expired but added late"));
    }
}
