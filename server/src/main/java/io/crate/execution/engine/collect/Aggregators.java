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

package io.crate.execution.engine.collect;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.lucene.index.LeafReaderContext;

import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.AggregationProjection;
import io.crate.execution.engine.collect.collectors.SegmentBatchIterator;
import io.crate.expression.symbol.Symbol;

public final class Aggregators {

    public Aggregators(DocInputFactory docInputFactory, List<Symbol> collect, AggregationProjection aggregationProjection) {
    }

    public BatchIterator<Row> createBatchIterator(LeafReaderContext leaf, SegmentBatchIterator segmentBatchIterator) {
        return CollectingBatchIterator.newInstance(
            segmentBatchIterator::close,
            segmentBatchIterator::kill,
            () -> CompletableFuture.completedFuture(loadItems(segmentBatchIterator)),
            true
        );
    }

    public Iterable<? extends Row> loadItems(SegmentBatchIterator source) {
        return List.of();
    }
}
