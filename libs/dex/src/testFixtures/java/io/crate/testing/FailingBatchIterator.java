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

import io.crate.data.BatchIterator;
import io.crate.data.ForwardingBatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.exceptions.Exceptions;

public class FailingBatchIterator<T> extends ForwardingBatchIterator<T> {

    private final BatchIterator<T> delegate;
    private final int failAfter;
    private int moveNextCalls = 0;

    public static <T> BatchIterator<T> failOnAllLoaded() {
        BatchIterator<T> delegate = InMemoryBatchIterator.empty(null);
        return new ForwardingBatchIterator<T>() {
            @Override
            protected BatchIterator<T> delegate() {
                return delegate;
            }

            @Override
            public boolean allLoaded() {
                Exceptions.rethrowUnchecked(new InterruptedException("Job killed"));
                return true;
            }
        };
    }

    public FailingBatchIterator(BatchIterator<T> delegate, int failAfter) {
        this.delegate = delegate;
        this.failAfter = failAfter;
    }

    @Override
    public void moveToStart() {
        super.moveToStart();
        moveNextCalls = 0;
    }

    @Override
    public boolean moveNext() {
        boolean moveNext = super.moveNext();
        moveNextCalls++;
        if (moveNextCalls == failAfter) {
            throw new UnsupportedOperationException("Fail after " + moveNextCalls + " moveNext calls");
        }
        return moveNext;
    }

    @Override
    protected BatchIterator<T> delegate() {
        return delegate;
    }
}
