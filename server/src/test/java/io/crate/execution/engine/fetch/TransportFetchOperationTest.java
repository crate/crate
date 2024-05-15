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

package io.crate.execution.engine.fetch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntObjectHashMap;

import io.crate.data.breaker.BlockBasedRamAccounting;
import io.crate.data.breaker.RamAccounting;

public class TransportFetchOperationTest extends ESTestCase {

    @Test
    public void test_no_ram_accounting_on_empty_fetch_ids_and_close() {
        RamAccounting ramAccounting = TransportFetchOperation.ramAccountingForIncomingResponse(
            RamAccounting.NO_ACCOUNTING,
            new IntObjectHashMap<>(),
            true);
        assertThat(ramAccounting, is(RamAccounting.NO_ACCOUNTING));
    }

    @Test
    public void test_ram_accounting_on_non_empty_fetch_ids_and_close() {
        var toFetch = new IntObjectHashMap<IntContainer>();
        toFetch.put(1, new IntArrayList());
        RamAccounting ramAccounting = TransportFetchOperation.ramAccountingForIncomingResponse(
            RamAccounting.NO_ACCOUNTING,
            toFetch,
            true);
        assertThat(ramAccounting).isExactlyInstanceOf(BlockBasedRamAccounting.class);
    }

}
