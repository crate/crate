/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.action;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.StreamSupport;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.common.unit.TimeValue;


public class LimitedBackoffPolicyTest extends ESTestCase {

    @Test
    public void testNoNext() throws Exception {
        Iterable<TimeValue> policy = BackoffPolicy.exponentialBackoff(0, 1, Integer.MAX_VALUE);
        Iterator<TimeValue> it = policy.iterator();
        it.next();
        assertThatThrownBy(() -> it.next())
            .isExactlyInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void testStartValue() throws Exception {
        Iterable<TimeValue> policy = BackoffPolicy.exponentialBackoff(100, 3, Integer.MAX_VALUE);
        assertThat(policy).containsExactly(
            TimeValue.timeValueMillis(110),
            TimeValue.timeValueMillis(130),
            TimeValue.timeValueMillis(200)
        );
    }

    @Test
    public void testLimit() throws Exception {
        int maxDelay = 1000;
        Iterable<TimeValue> policy = BackoffPolicy.exponentialBackoff(0, 1000, maxDelay);
        for (TimeValue val : policy) {
            assertThat(val.millis()).isLessThanOrEqualTo(maxDelay);
        }

        Iterable<TimeValue> exponentialBackoff = BackoffPolicy.exponentialBackoff(50, 800, 5000);
        TimeValue total = TimeValue.timeValueMillis(StreamSupport.stream(exponentialBackoff.spliterator(), false)
            .mapToLong(timeValue -> timeValue.millis())
            .sum());
        assertThat(total).isLessThan(TimeValue.timeValueMinutes(72));
    }

    @Test
    public void test_timeout_limit() throws Exception {
        Iterable<TimeValue> policy = BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(50), TimeValue.timeValueMillis(300));
        assertThat(policy).containsExactly(
            TimeValue.timeValueMillis(60),
            TimeValue.timeValueMillis(80),
            TimeValue.timeValueMillis(150),
            TimeValue.timeValueMillis(280)
        );
    }
}
