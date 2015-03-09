/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.merge;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;

class FutureBackedRowIterator implements Iterator<Row> {

    private final ListenableFuture<Bucket> bucketFuture;
    private Iterator<Row> bucketIt = null;

    public FutureBackedRowIterator(ListenableFuture<Bucket> bucketFuture) {
        this.bucketFuture = bucketFuture;
    }

    @Override
    public boolean hasNext() {
        waitForFuture();
        return bucketIt.hasNext();
    }

    private void waitForFuture() {
        if (bucketIt == null) {
            try {
                bucketIt = bucketFuture.get().iterator();
            } catch (InterruptedException | ExecutionException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    @Override
    public Row next() {
        waitForFuture();
        return bucketIt.next();
    }

    @Override
    public void remove() {
        waitForFuture();
        bucketIt.remove();
    }
}
