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

import java.util.function.LongSupplier;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeUnit;

import io.crate.breaker.EstimateCellsSize;
import io.crate.data.AsyncFlatMapBatchIterator;
import io.crate.data.BatchIterator;
import io.crate.data.BatchIterators;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.dsl.projection.FetchProjection;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;

public final class FetchProjector {

    private static final long MIN_BYTES_PER_BUCKETS = ByteSizeUnit.KB.toBytes(64);

    // We only know the memory of the incoming rows up-front, so the percentage is low
    // to leave space for the cells being fetched.
    private static final double BREAKER_LIMIT_PERCENTAGE = 0.20d;

    public static long computeReaderBucketsByteThreshold(CircuitBreaker circuitBreaker) {
        return Math.max(
            (long) (circuitBreaker.getFree() * BREAKER_LIMIT_PERCENTAGE),
            MIN_BYTES_PER_BUCKETS
        );
    }

    public static Projector create(FetchProjection projection,
                                   RamAccounting ramAccounting,
                                   LongSupplier getBucketsBytesThreshold,
                                   TransactionContext txnCtx,
                                   NodeContext nodeCtx,
                                   FetchOperation fetchOperation) {
        final FetchRows fetchRows = FetchRows.create(
            txnCtx,
            nodeCtx,
            projection.fetchSources(),
            projection.outputSymbols()
        );
        EstimateCellsSize estimateRowSize = new EstimateCellsSize(projection.inputTypes());
        return (BatchIterator<Row> source) -> {
            final long maxBucketsSizeInBytes = getBucketsBytesThreshold.getAsLong();
            BatchIterator<ReaderBuckets> buckets = BatchIterators.chunks(
                source,
                projection.getFetchSize(),
                () -> new ReaderBuckets(fetchRows, projection::getFetchSourceByReader, estimateRowSize, ramAccounting),
                ReaderBuckets::add,
                readerBuckets -> readerBuckets.ramBytesUsed() > maxBucketsSizeInBytes
            );
            return new AsyncFlatMapBatchIterator<>(
                buckets,
                new FetchMapper(fetchOperation, projection.nodeReaders())
            );
        };
    }
}
