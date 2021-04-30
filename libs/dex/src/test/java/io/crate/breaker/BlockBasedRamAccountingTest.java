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

import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class BlockBasedRamAccountingTest {

    @Test
    public void test_block_based_ram_accounting_reserves_2mb_block_on_first_add_byte_call() {
        var requestedBytes = new AtomicLong(0L);
        var ramAccounting = new BlockBasedRamAccounting(requestedBytes::addAndGet, 4096);

        ramAccounting.addBytes(40);
        assertThat(ramAccounting.totalBytes(), is(40L));
        assertThat(requestedBytes.get(), is((long) 4096));
    }

    @Test
    public void test_requested_bytes_are_released_on_release() {
        var requestedBytes = new AtomicLong(0L);
        var ramAccounting = new BlockBasedRamAccounting(requestedBytes::addAndGet, 2048);

        ramAccounting.addBytes(40);
        ramAccounting.addBytes(40);
        assertThat(requestedBytes.get(), is((long) 2048));
        ramAccounting.release();
        assertThat(requestedBytes.get(), is(0L));
    }

    @Test
    public void test_bytes_are_exact_accounted_if_block_size_is_smaller_than_requested_bytes() {
        var accountedBytes = new AtomicLong(0L);
        var ramAccounting = new BlockBasedRamAccounting(accountedBytes::addAndGet, 200);

        ramAccounting.addBytes(5432);
        assertThat(accountedBytes.get(), is(5432L));
    }
}
