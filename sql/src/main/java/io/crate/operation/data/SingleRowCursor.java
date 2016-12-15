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

public class SingleRowCursor implements BatchCursor{

    private volatile Status status = Status.ON_ROW;
    private final Object[] data;

    public SingleRowCursor(Object[] data) {
        this.data = data;
    }

    public static SingleRowCursor of(Object ... data) {
        return new SingleRowCursor(data);
    }

    private void checkClosed(){
        if (status == Status.CLOSED){
            throw new IllegalStateException("Cursor is closed");
        }
    }

    @Override
    public boolean moveFirst() {
        status = Status.ON_ROW;
        return true;
    }

    @Override
    public boolean moveNext() {
        status = Status.OFF_ROW;
        return false;
    }

    @Override
    public void close() {
        checkClosed();
        status = Status.CLOSED;
    }

    @Override
    public Status status() {
        return status;
    }

    @Override
    public ListenableFuture<?> loadNextBatch() {
        throw new IllegalStateException("Single row cursor has no more batches");
    }

    @Override
    public boolean allLoaded() {
        return true;
    }

    @Override
    public int size() {
        return data.length;
    }

    @Override
    public Object get(int index) {
        if (status == Status.ON_ROW) {
            return data[index];
        }
        throw new IllegalStateException("get called on invalid status " + status);
    }

    @Override
    public Object[] materialize() {
        Object[] copy = new Object[data.length];
        System.arraycopy(data, 0, copy, 0, data.length);
        return copy;
    }
}
