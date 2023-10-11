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

package io.crate.breaker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.junit.jupiter.api.Test;

public class ConcurrentRamAccountingTest {

    @Test
    void test_raises_circuit_breaking_exception_if_limit_is_exceeded() {
        AtomicLong usedBytes = new AtomicLong();
        var ramAccounting = new ConcurrentRamAccounting(usedBytes::addAndGet, ignore -> {}, "dummy", 20);
        ramAccounting.addBytes(19);
        assertThatThrownBy(() -> ramAccounting.addBytes(5))
            .isExactlyInstanceOf(CircuitBreakingException.class)
            .hasMessage("\"dummy\" reached operation memory limit. Used: 24b, Limit: 20b");

        assertThat(usedBytes).as("value is not reserved if above limit").hasValue(19);
        assertThat(ramAccounting.totalBytes()).isEqualTo(19);
    }
}

