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

package io.crate.data;

import java.util.concurrent.CompletionStage;

public class SkippingBatchIterator implements BatchIterator {

    private final BatchIterator delegate;
    private final int offset;

    private int skipped = 0;

    public static BatchIterator newInstance(BatchIterator delegate, int offset) {
        return new CloseAssertingBatchIterator(new SkippingBatchIterator(delegate, offset));
    }

    private SkippingBatchIterator(BatchIterator delegate, int offset) {
        this.delegate = delegate;
        this.offset = offset;
    }

    @Override
    public void moveToStart() {
        skipped = 0;
        delegate.moveToStart();
    }

    @Override
    public boolean moveNext() {
        if (skipped < offset) {
            for (; skipped < offset; skipped++) {
                if (delegate.moveNext() == false) {
                    return false;
                }
            }
        }
        return delegate.moveNext();
    }

    @Override
    public Row currentRow() {
        return delegate.currentRow();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        return delegate.loadNextBatch();
    }

    @Override
    public boolean allLoaded() {
        return delegate.allLoaded();
    }
}
