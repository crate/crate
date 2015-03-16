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

package io.crate.executor.transport.distributed;

import com.google.common.base.Throwables;
import io.crate.Streamer;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.executor.transport.StreamBucket;

import java.io.IOException;


public class SingleBucketBuilder extends ResultProviderBase {

    private final StreamBucket.Builder bucketBuilder;

    public SingleBucketBuilder(Streamer<?>[] streamers) {
        bucketBuilder = new StreamBucket.Builder(streamers);
    }

    public Bucket build() {
        try {
            return bucketBuilder.build();
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        return null;
    }

    @Override
    public Bucket doFinish() {
        return build();
    }

    @Override
    public Throwable doFail(Throwable t) {
        return t;
    }

    @Override
    public synchronized boolean setNextRow(Row row) {
        try {
            bucketBuilder.add(row);
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        return true;
    }
}
