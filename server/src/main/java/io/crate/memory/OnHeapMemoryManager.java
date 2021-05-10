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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.function.IntConsumer;

/**
 * A MemoryManager that allocates buffers on the heap.
 * It doesn't account the used memory directly, but indirectly via an injected `IntConsumer`.
 * This IntConsumer is also responsible to de-account the used-bytes eventually.
 */
public class OnHeapMemoryManager implements MemoryManager {

    private final IntConsumer accountBytes;

    /**
     * @param accountBytes A consumer that will be called on each ByteBuf allocation with the number of allocated bytes.
     */
    public OnHeapMemoryManager(IntConsumer accountBytes) {
        this.accountBytes = accountBytes;
    }

    @Override
    public ByteBuf allocate(int capacity) {
        accountBytes.accept(capacity);
        // We don't track the ByteBuf instance to release it later because it is not necessary for on-heap buffers.
        return Unpooled.buffer(capacity);
    }

    @Override
    public void close() {
    }
}
