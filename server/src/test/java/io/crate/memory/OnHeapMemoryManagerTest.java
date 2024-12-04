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

package io.crate.memory;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

public class OnHeapMemoryManagerTest {

    @Test
    public void test_allocated_bytebuffers_are_accounted() {
        var bytes = new AtomicLong();
        var memoryManager = new OnHeapMemoryManager(bytes::addAndGet);

        memoryManager.allocate(20);
        assertThat(bytes.get()).isEqualTo(20L);

        // Closing the memoryManager doesn't de-account the bytes. That responsibility is delegated to the IntConsumer
        memoryManager.close();
        assertThat(bytes.get()).isEqualTo(20L);
    }
}
