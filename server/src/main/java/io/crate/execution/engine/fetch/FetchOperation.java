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

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectMap;
import io.crate.data.Bucket;

import java.util.concurrent.CompletableFuture;

public interface FetchOperation {

    /**
     * @param nodeId       the nodeId of the node from which to fetch from
     * @param toFetch      a map from readerIds to docIds which should be fetched
     * @param closeContext indicate if context must be closed after fetch
     */
    CompletableFuture<IntObjectMap<? extends Bucket>> fetch(String nodeId,
                                                            IntObjectMap<IntArrayList> toFetch,
                                                            boolean closeContext);
}
