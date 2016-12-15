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

package io.crate.operation.data;

import com.google.common.util.concurrent.ListenableFuture;

public class EmptyBatchCursor implements BatchCursor{

    private volatile boolean closed = false;

    @Override
    public boolean moveFirst() {
        return false;
    }

    @Override
    public boolean moveNext() {
        return false;
    }

    @Override
    public void close() {
        if (closed){
            throw new IllegalStateException("Cursor is already closed");
        }
        closed = true;
    }

    @Override
    public Status status() {
        if (closed){
            return Status.CLOSED;
        } else {
            return Status.OFF_ROW;
        }
    }

    @Override
    public ListenableFuture<?> loadNextBatch() {
        throw new IllegalStateException("Empty cursor has no more batches");
    }

    @Override
    public boolean allLoaded() {
        return true;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public Object get(int index) {
        throw new IllegalStateException("Not on a row");
    }

    @Override
    public Object[] materialize() {
        throw new IllegalStateException("Not on a row");
    }
}
