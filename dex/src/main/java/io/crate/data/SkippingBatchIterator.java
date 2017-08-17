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

public class SkippingBatchIterator<T> extends ForwardingBatchIterator<T> {

    private final BatchIterator<T> delegate;
    private final int offset;
    private int skipped = 0;

    public SkippingBatchIterator(BatchIterator<T> delegate, int offset) {
        this.delegate = delegate;
        this.offset = offset;
    }

    @Override
    protected BatchIterator<T> delegate() {
        return delegate;
    }

    @Override
    public void moveToStart() {
        super.moveToStart();
        skipped = 0;
    }

    @Override
    public boolean moveNext() {
        for (; skipped < offset; skipped++) {
            if (delegate.moveNext() == false) {
                return false;
            }
        }
        return delegate.moveNext();
    }
}
