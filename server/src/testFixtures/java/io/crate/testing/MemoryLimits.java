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

package io.crate.testing;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.util.function.Supplier;

import com.sun.management.ThreadMXBean;

public final class MemoryLimits {

    /**
     * Asserts that `supplier.get` doesn't allocate more than `bytes` bytes.
     * This is limited to the current thread and cannot account for memory accounted in other threads.
     */
    public static <T> T assertMaxBytesAllocated(long bytes, Supplier<T> supplier) {
        ThreadMXBean threadMXBean = ManagementFactory.getPlatformMXBean(ThreadMXBean.class);
        long threadId = Thread.currentThread().threadId();
        long allocatedBytesBegin = threadMXBean.getThreadAllocatedBytes(threadId);
        T t = supplier.get();
        long allocatedBytesAfter = threadMXBean.getThreadAllocatedBytes(threadId);
        long allocatedBytes = allocatedBytesAfter - allocatedBytesBegin;
        assertThat(allocatedBytes).isLessThanOrEqualTo(bytes);
        return t;
    }
}
