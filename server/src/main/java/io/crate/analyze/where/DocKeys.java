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

package io.crate.analyze.where;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.Id;
import io.crate.analyze.SymbolEvaluator;
import io.crate.common.StringUtils;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.types.DataTypes;
import io.crate.types.LongType;

public class DocKeys implements Iterable<DocKeys.DocKey> {

    private final int width;
    private final Function<List<String>, String> idFunction;
    private final boolean withSequenceVersioning;
    private int clusteredByIdx;
    private final boolean withVersions;
    private final List<List<Symbol>> docKeys;
    private final List<Integer> partitionIdx;

    public class DocKey {

        private final List<Symbol> key;

        private DocKey(int pos) {
            key = docKeys.get(pos);
        }

        public String getId(TransactionContext txnCtx, NodeContext nodeCtx, Row params, SubQueryResults subQueryResults) {
            return idFunction.apply(
                Lists.mapLazy(
                    key.subList(0, width),
                    s -> StringUtils.nullOrString(SymbolEvaluator.evaluate(txnCtx, nodeCtx, s, params, subQueryResults))
                )
            );
        }

        public Optional<Long> version(TransactionContext txnCtx, NodeContext nodeCtx, Row params, SubQueryResults subQueryResults) {
            if (withVersions && key.get(width) != null) {
                Object val = SymbolEvaluator.evaluate(txnCtx, nodeCtx, key.get(width), params, subQueryResults);
                return Optional.of(DataTypes.LONG.sanitizeValue(val));
            }
            return Optional.empty();
        }

        public Optional<Long> sequenceNo(TransactionContext txnCtx, NodeContext nodeCtx, Row params, SubQueryResults subQueryResults) {
            if (withSequenceVersioning && key.get(width) != null) {
                Object val = SymbolEvaluator.evaluate(txnCtx, nodeCtx, key.get(width), params, subQueryResults);
                return Optional.of(LongType.INSTANCE.sanitizeValue(val));
            }
            return Optional.empty();
        }

        public Optional<Long> primaryTerm(TransactionContext txnCtx, NodeContext nodeCtx, Row params, SubQueryResults subQueryResults) {
            if (withSequenceVersioning && key.get(width + 1) != null) {
                Object val = SymbolEvaluator.evaluate(txnCtx, nodeCtx, key.get(width + 1), params, subQueryResults);
                return Optional.of(LongType.INSTANCE.sanitizeValue(val));
            }
            return Optional.empty();
        }

        public List<Symbol> values() {
            return key;
        }

        public List<String> getPartitionValues(TransactionContext txnCtx, NodeContext nodeCtx, Row params, SubQueryResults subQueryResults) {
            if (partitionIdx == null || partitionIdx.isEmpty()) {
                return Collections.emptyList();
            }
            return Lists.map(
                partitionIdx,
                pIdx -> DataTypes.STRING.implicitCast(SymbolEvaluator.evaluate(txnCtx, nodeCtx, key.get(pIdx), params, subQueryResults)));

        }

        public String getRouting(TransactionContext txnCtx, NodeContext nodeCtx, Row params, SubQueryResults subQueryResults) {
            if (clusteredByIdx >= 0) {
                return SymbolEvaluator.evaluate(txnCtx, nodeCtx, key.get(clusteredByIdx), params, subQueryResults).toString();
            }
            return getId(txnCtx, nodeCtx, params, subQueryResults);
        }
    }

    public DocKeys(List<List<Symbol>> docKeys,
                   boolean withVersions,
                   boolean withSequenceVersioning,
                   int clusteredByIdx,
                   @Nullable List<Integer> partitionIdx) {
        this.partitionIdx = partitionIdx;
        assert docKeys != null && !docKeys.isEmpty() : "docKeys must not be null nor empty";
        if (withVersions) {
            this.width = docKeys.get(0).size() - 1;
        } else if (withSequenceVersioning) {
            this.width = docKeys.get(0).size() - 2;
        } else {
            this.width = docKeys.get(0).size();
        }
        this.withVersions = withVersions;
        this.withSequenceVersioning = withSequenceVersioning;
        this.docKeys = docKeys;
        this.clusteredByIdx = clusteredByIdx;
        this.idFunction = Id.compile(width, clusteredByIdx);
    }

    public DocKey getOnlyKey() {
        if (size() != 1) {
            throw new IllegalArgumentException("must contain only one key");
        }
        return new DocKey(0);
    }

    public int size() {
        return docKeys.size();
    }

    @Override
    public Iterator<DocKey> iterator() {
        return new Iterator<>() {
            int i = 0;

            @Override
            public boolean hasNext() {
                return i < docKeys.size();
            }

            @Override
            public DocKey next() {
                return new DocKey(i++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported for " +
                                                        DocKeys.class.getSimpleName() + "$iterator");
            }
        };
    }

    @Override
    public String toString() {
        return "DocKeys{" + docKeys.stream()
            .map(xs -> Lists.joinOn(", ", xs, Symbol::toString))
            .sorted()
            .collect(Collectors.joining("; ")) + '}';
    }
}
