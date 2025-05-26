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

package io.crate.execution.engine.aggregation;

import java.io.IOException;

import org.jetbrains.annotations.Nullable;

import io.crate.data.breaker.RamAccounting;
import io.crate.memory.MemoryManager;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.Version;

public interface DocValueAggregator<T> {

    T initialState(RamAccounting ramAccounting, MemoryManager memoryManager, Version minNodeVersion);

    void loadDocValues(LeafReaderContext leafReaderContext) throws IOException;

    void apply(RamAccounting ramAccounting, int doc, T state) throws IOException;

    // Aggregations are executed on shard level,
    // that means there is always a final reduce step necessary
    // → never return final value, but always partial result
    @Nullable
    Object partialResult(RamAccounting ramAccounting, T state);
}
