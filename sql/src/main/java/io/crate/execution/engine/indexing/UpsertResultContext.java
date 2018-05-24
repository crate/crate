/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.indexing;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.SourceIndexWriterReturnSummaryProjection;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.InputFactory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

public class UpsertResultContext {

    public static UpsertResultContext forRowCount() {
        return new UpsertResultContext(
            () -> null, () -> null, Collections.emptyList(), UpsertResultCollectors.newRowCountCollector()) {

            @Override
            BiConsumer<ShardedRequests, String> getItemFailureRecorder() {
                return (s, f) -> { };
            }

            @Override
            Predicate<ShardedRequests> getHasSourceUriFailureChecker() {
                return (ignored) -> false;
            }
        };
    }

    public static UpsertResultContext forReturnSummary(SourceIndexWriterReturnSummaryProjection projection,
                                                       DiscoveryNode discoveryNode,
                                                       InputFactory inputFactory) {
        InputFactory.Context<CollectExpression<Row, ?>> ctxSourceInfo = inputFactory.ctxForInputColumns();
        //noinspection unchecked
        Input<BytesRef> sourceUriInput = (Input<BytesRef>) ctxSourceInfo.add(projection.sourceUri());
        //noinspection unchecked
        Input<String> sourceUriFailureInput = (Input<String>) ctxSourceInfo.add(projection.sourceUriFailure());

        return new UpsertResultContext(
            sourceUriInput,
            sourceUriFailureInput,
            ctxSourceInfo.expressions(),
            UpsertResultCollectors.newSummaryCollector(discoveryNode));
    }


    private final Input<BytesRef> sourceUriInput;
    private final Input<String> sourceUriFailureInput;
    private final List<? extends CollectExpression<Row, ?>> sourceInfoExpressions;
    private final UpsertResultCollector resultCollector;

    private UpsertResultContext(Input<BytesRef> sourceUriInput,
                                Input<String> sourceUriFailureInput,
                                List<? extends CollectExpression<Row, ?>> sourceInfoExpressions,
                                UpsertResultCollector resultCollector) {
        this.sourceUriInput = sourceUriInput;
        this.sourceUriFailureInput = sourceUriFailureInput;
        this.sourceInfoExpressions = sourceInfoExpressions;
        this.resultCollector = resultCollector;
    }

    Input<BytesRef> getSourceUriInput() {
        return sourceUriInput;
    }

    List<? extends CollectExpression<Row, ?>> getSourceInfoExpressions() {
        return sourceInfoExpressions;
    }

    UpsertResultCollector getResultCollector() {
        return resultCollector;
    }

    BiConsumer<ShardedRequests, String> getItemFailureRecorder() {
        return (s, f) -> s.addFailedItem(sourceUriInput.value(), f);
    }

    Predicate<ShardedRequests> getHasSourceUriFailureChecker() {
        return s -> {
            String sourceUriFailure = sourceUriFailureInput.value();
            if (sourceUriFailure != null) {
                s.addFailedUri(sourceUriInput.value(), sourceUriFailureInput.value());
                return true;
            }
            return false;
        };
    }
}
