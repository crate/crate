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

package io.crate.jobs;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.analyze.WhereClause;
import io.crate.data.BatchConsumer;
import io.crate.data.Row1;
import io.crate.data.RowsBatchIterator;
import io.crate.operation.count.CountOperation;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CountContext extends AbstractExecutionSubContext {

    private static final Logger LOGGER = Loggers.getLogger(CountContext.class);

    private final CountOperation countOperation;
    private final BatchConsumer consumer;
    private final Map<String, List<Integer>> indexShardMap;
    private final WhereClause whereClause;
    private ListenableFuture<Long> countFuture;

    public CountContext(int id,
                        CountOperation countOperation,
                        BatchConsumer consumer,
                        Map<String, List<Integer>> indexShardMap,
                        WhereClause whereClause) {
        super(id, LOGGER);
        this.countOperation = countOperation;
        this.consumer = consumer;
        this.indexShardMap = indexShardMap;
        this.whereClause = whereClause;
    }

    @Override
    public synchronized void innerStart() {
        try {
            countFuture = countOperation.count(indexShardMap, whereClause);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        Futures.addCallback(countFuture, new FutureCallback<Long>() {
            @Override
            public void onSuccess(@Nullable Long result) {
                consumer.accept(
                    RowsBatchIterator.newInstance(Collections.singletonList(new Row1(result)), 1), null);
                close();
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                consumer.accept(null, t);
                close(t);
            }
        });
    }

    @Override
    public synchronized void innerKill(@Nonnull Throwable throwable) {
        if (countFuture == null) {
            consumer.accept(null, throwable);
        } else {
            countFuture.cancel(true);
        }
    }

    @Override
    protected void innerClose(@Nullable Throwable t) {
    }

    @Override
    public String name() {
        return "count(*)";
    }
}
