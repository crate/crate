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

package io.crate.operation.join;

import io.crate.data.BatchIterator;
import io.crate.data.ForwardingBatchIterator;

import java.util.concurrent.CompletableFuture;

public class ListenableBatchIterator extends ForwardingBatchIterator {

    private final BatchIterator delegate;
    private final CompletableFuture<Void> completeOnClose;

    public ListenableBatchIterator(BatchIterator delegate, CompletableFuture<Void> completeOnClose) {
        this.delegate = delegate;
        this.completeOnClose = completeOnClose;
    }

    @Override
    protected BatchIterator delegate() {
        return delegate;
    }

    @Override
    public void close() {
        super.close();
        completeOnClose.complete(null);
    }
}
