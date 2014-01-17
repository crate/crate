/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.action;

import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.HandlesStreamInput;

import java.io.IOException;
import java.util.*;

public class ReduceJobRequestContext {

    private final Map<UUID, ReduceJobContext> reduceJobs = new HashMap<>();
    private final Map<UUID, List<BytesReference>> unreadStreams = new HashMap<>();
    private final Object lock = new Object();
    private final CacheRecycler cacheRecycler;

    public CacheRecycler cacheRecycler() {
        return cacheRecycler;
    }

    public ReduceJobRequestContext(CacheRecycler cacheRecycler) {
        this.cacheRecycler = cacheRecycler;
    }

    public ReduceJobContext get(UUID contextId) {
        synchronized (lock) {
            return reduceJobs.get(contextId);
        }
    }

    public void remove(UUID contextId) {
        reduceJobs.remove(contextId);
    }

    public void put(UUID contextId, ReduceJobContext status) throws IOException {
        List<BytesReference> bytesReferences;
        synchronized (lock) {
            reduceJobs.put(contextId, status);
            bytesReferences = unreadStreams.get(contextId);
        }

        if (bytesReferences != null) {
            for (BytesReference bytes : bytesReferences) {
                mergeFromBytesReference(bytes, status);
            }
        }
    }

    public void push(final SQLMapperResultRequest request) throws IOException {
        if (request.groupByResult != null) {
            request.status.merge(request.groupByResult);
            return;
        }

        synchronized (lock) {
            ReduceJobContext status = reduceJobs.get(request.contextId);
            if (request.failed) {
                status.countFailure();
                return;
            }
            if (status == null) {
                List<BytesReference> bytesStreamOutputs = unreadStreams.get(request.contextId);
                if (bytesStreamOutputs == null) {
                    bytesStreamOutputs = new ArrayList<>();
                    unreadStreams.put(request.contextId, bytesStreamOutputs);
                }
                bytesStreamOutputs.add(request.memoryOutputStream.bytes());
            } else {
                mergeFromBytesReference(request.memoryOutputStream.bytes(), status);
            }
        }
    }

    private void mergeFromBytesReference(BytesReference bytesReference, ReduceJobContext status) throws IOException {
        SQLGroupByResult sqlGroupByResult = SQLGroupByResult.readSQLGroupByResult(
            status.parsedStatement,
            cacheRecycler,
            // required to wrap into HandlesStreamInput because it has a different readString()
            // implementation than BytesStreamInput alone.
            // the memoryOutputStream originates from a HandlesStreamOutput
            new HandlesStreamInput(new BytesStreamInput(bytesReference))
        );
        status.merge(sqlGroupByResult);
    }
}

