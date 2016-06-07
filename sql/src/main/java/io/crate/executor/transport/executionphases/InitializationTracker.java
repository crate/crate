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

package io.crate.executor.transport.executionphases;

import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicInteger;

class InitializationTracker {

    final SettableFuture<Void> future;
    private final AtomicInteger serverToInitialize;
    private Throwable failure;

    InitializationTracker(int numServer) {
        future = SettableFuture.create();
        if (numServer == 0) {
            future.set(null);
        }
        serverToInitialize = new AtomicInteger(numServer);
    }

    void jobInitialized(@Nullable Throwable t) {
        if (failure == null || failure instanceof InterruptedException) {
            failure = t;
        }
        if (serverToInitialize.decrementAndGet() <= 0) {
            if (failure == null) {
                future.set(null);
            } else {
                future.setException(failure);
            }
        }
    }
}
