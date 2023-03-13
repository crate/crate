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

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.common.unit.TimeValue;


public class LimitedBackoffPolicyTest extends ESTestCase {

    @Test
    public void testNoNext() throws Exception {
        BackoffPolicy policy = new LimitedExponentialBackoff(0, 1, Integer.MAX_VALUE);
        Iterator<TimeValue> it = policy.iterator();
        it.next();
        assertThatThrownBy(() -> it.next())
            .isExactlyInstanceOf(NoSuchElementException.class)
            .hasMessage("Reached maximum amount of backoff iterations. Only 1 iterations allowed.");
    }

    @Test
    public void testStartValue() throws Exception {
        LimitedExponentialBackoff policy = new LimitedExponentialBackoff(100, 1, Integer.MAX_VALUE);
        Iterator<TimeValue> it = policy.iterator();
        assertThat(it.next()).isEqualTo(TimeValue.timeValueMillis(100));
    }

    @Test
    public void testLimit() throws Exception {
        int maxDelay = 1000;
        LimitedExponentialBackoff policy = new LimitedExponentialBackoff(0, 1000, maxDelay);
        for (TimeValue val : policy) {
            assertThat(val.millis()).isLessThanOrEqualTo(maxDelay);
        }
    }
}
