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

package io.crate.execution.engine.fetch;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.crate.common.concurrent.CompletableFutures;
import io.crate.data.AsyncFlatMapper;
import io.crate.data.Bucket;
import io.crate.data.CloseableIterator;
import io.crate.data.Row;

public class FetchMapper implements AsyncFlatMapper<ReaderBuckets, Row> {

    private static final Logger LOGGER = LogManager.getLogger(FetchMapper.class);

    private final FetchOperation fetchOperation;
    private final Map<String, IntSet> readerIdsByNode;

    public FetchMapper(FetchOperation fetchOperation, Map<String, IntSet> readerIdsByNode) {
        this.fetchOperation = fetchOperation;
        this.readerIdsByNode = readerIdsByNode;
    }

    @Override
    public CompletableFuture<? extends CloseableIterator<Row>> apply(ReaderBuckets readerBuckets, boolean isLastCall) {
        List<CompletableFuture<IntObjectMap<? extends Bucket>>> futures = new ArrayList<>();
        Iterator<Map.Entry<String, IntSet>> it = readerIdsByNode.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, IntSet> entry = it.next();
            IntObjectHashMap<IntArrayList> toFetch = readerBuckets.generateToFetch(entry.getValue());
            if (toFetch.isEmpty() && !isLastCall) {
                continue;
            }
            final String nodeId = entry.getKey();
            try {
                futures.add(fetchOperation.fetch(nodeId, toFetch, isLastCall));
            } catch (Throwable t) {
                futures.add(CompletableFuture.failedFuture(t));
            }
            if (isLastCall) {
                it.remove();
            }
        }
        return CompletableFutures.allAsList(futures).thenApply(readerBuckets::getOutputRows);
    }

    @Override
    public void close() {
        for (String nodeId : readerIdsByNode.keySet()) {
            fetchOperation.fetch(nodeId, new IntObjectHashMap<>(0), true)
                .exceptionally(e -> {
                    LOGGER.error("An error happened while sending close fetchRequest to node=" + nodeId, e);
                    return null;
                });
        }
    }
}
