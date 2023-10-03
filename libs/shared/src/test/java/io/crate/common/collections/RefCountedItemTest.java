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

package io.crate.common.collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.Test;


public class RefCountedItemTest {

    @Test
    public void test_failing_factory_does_not_increment_ref_count() throws Exception {
        AtomicInteger count = new AtomicInteger(0);
        AtomicBoolean closeCalled = new AtomicBoolean(false);
        Function<String, Integer> failOnFirst = source -> {
            if (count.incrementAndGet() == 1) {
                throw new IllegalArgumentException("dummy");
            }
            return 1;
        };
        RefCountedItem<Integer> item = new RefCountedItem<>(failOnFirst, x -> closeCalled.set(true));
        assertThatThrownBy(() -> item.markAcquired("dummy1"))
            .isExactlyInstanceOf(IllegalArgumentException.class);

        item.markAcquired("dummy2");
        item.close();
        assertThat(closeCalled.get())
            .as("Close must have been called because ref-count was only incremented once")
            .isTrue();
    }
}
