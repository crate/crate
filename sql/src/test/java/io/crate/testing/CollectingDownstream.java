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

package io.crate.testing;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.base.Throwables;
import io.crate.core.collections.Buckets;
import io.crate.core.collections.CollectionBucket;
import io.crate.core.collections.Row;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;

import java.util.Vector;

public class CollectingDownstream implements RowDownstream, RowDownstreamHandle {

    private final Vector<Object[]> rows = new Vector<>();
    private CollectionBucket bucket;

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        return this;
    }

    @Override
    public synchronized boolean setNextRow(Row row) {
        rows.add(Buckets.materialize(row));
        return true;
    }

    @Override
    public synchronized void finish() {
        assert bucket == null;
        bucket = new CollectionBucket(rows);
    }

    @Override
    public void fail(Throwable throwable) {
        Throwables.propagate(throwable);
    }

    public CollectionBucket bucket() {
        return bucket;
    }
}
