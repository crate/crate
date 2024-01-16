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

package io.crate.execution.dsl.projection;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.monitor.jvm.JvmInfo;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.IntCursor;

import io.crate.Streamer;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists;
import io.crate.common.collections.MapBuilder;
import io.crate.data.Paging;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.RelationName;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.types.DataType;

public class FetchProjection extends Projection {

    private static final int HEAP_IN_MB = (int) (JvmInfo.jvmInfo().getConfiguredMaxHeapSize() / (1024 * 1024));
    private static final int MAX_FETCH_SIZE = maxFetchSize(HEAP_IN_MB);

    private final int fetchPhaseId;
    private final int fetchSize;
    private final Map<RelationName, FetchSource> fetchSources;
    private final List<Symbol> outputSymbols;
    private final Map<String, IntSet> nodeReaders;
    private final TreeMap<Integer, String> readerIndices;
    private final Map<String, RelationName> indicesToIdents;
    private final List<DataType<?>> inputTypes;

    public FetchProjection(int fetchPhaseId,
                           int suppliedFetchSize,
                           Map<RelationName, FetchSource> fetchSources,
                           List<Symbol> outputSymbols,
                           List<DataType<?>> inputTypes,
                           Map<String, IntSet> nodeReaders,
                           TreeMap<Integer, String> readerIndices,
                           Map<String, RelationName> indicesToIdents) {
        assert outputSymbols.stream().noneMatch(s ->
            SymbolVisitors.any(x -> x instanceof ScopedSymbol || x instanceof SelectSymbol, s))
            : "Cannot operate on Field or SelectSymbol symbols: " + outputSymbols;
        this.fetchPhaseId = fetchPhaseId;
        this.fetchSources = fetchSources;
        this.outputSymbols = outputSymbols;
        this.inputTypes = inputTypes;
        this.nodeReaders = nodeReaders;
        this.readerIndices = readerIndices;
        this.indicesToIdents = indicesToIdents;
        this.fetchSize = boundedFetchSize(suppliedFetchSize, MAX_FETCH_SIZE);
    }

    @VisibleForTesting
    static int maxFetchSize(int maxHeapInMb) {
        /* These values were chosen after some manuel testing with a single node started with
         * different heap configurations:  56mb, 256mb and 2048mb
         *
         * This should result in fetchSizes that err on the safe-side to prevent out of memory issues.
         * And yet they get large quickly enough that  on a decently sized cluster it's on the maximum
         * (4gb heap already has a fetchSize of 500000)
         *
         * The returned maxFetchSize may still be too large in case of very large result payloads,
         * but there's still a circuit-breaker which should prevent OOM errors.
         */
        int x0 = 56;
        int y0 = 2000;
        int x1 = 256;
        int y1 = 30000;
        // linear interpolation formula.
        return Math.min(y0 + (maxHeapInMb - x0) * ((y1 - y0) / (x1 - x0)), Paging.PAGE_SIZE);
    }

    @VisibleForTesting
    static int boundedFetchSize(int suppliedFetchSize, int maxSize) {
        if (suppliedFetchSize == 0) {
            return maxSize;
        }
        return Math.min(suppliedFetchSize, maxSize);
    }

    public int fetchPhaseId() {
        return fetchPhaseId;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public Map<RelationName, FetchSource> fetchSources() {
        return fetchSources;
    }

    public List<Symbol> outputSymbols() {
        return outputSymbols;
    }

    public List<DataType<?>> inputTypes() {
        return inputTypes;
    }

    public Map<String, IntSet> nodeReaders() {
        return nodeReaders;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.FETCH;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitFetchProjection(this, context);
    }

    @Override
    public List<? extends Symbol> outputs() {
        return outputSymbols;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FetchProjection that = (FetchProjection) o;
        return fetchPhaseId == that.fetchPhaseId;
    }

    @Override
    public int hashCode() {
        return fetchPhaseId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("writeTo is not supported for " +
                                                FetchProjection.class.getSimpleName());
    }

    @Override
    public Map<String, Object> mapRepresentation() {
        return MapBuilder.<String, Object>newMapBuilder()
            .put("type", "Fetch")
            .put("outputs", Lists.joinOn(", ", outputSymbols, Symbol::toString))
            .put("fetchSize", fetchSize)
            .map();
    }

    public Map<String, ? extends IntObjectMap<Streamer<?>[]>> generateStreamersGroupedByReaderAndNode() {
        HashMap<String, IntObjectHashMap<Streamer<?>[]>> streamersByReaderByNode = new HashMap<>();
        for (Map.Entry<String, IntSet> entry : nodeReaders.entrySet()) {
            IntObjectHashMap<Streamer<?>[]> streamersByReaderId = new IntObjectHashMap<>();
            String nodeId = entry.getKey();
            streamersByReaderByNode.put(nodeId, streamersByReaderId);
            for (IntCursor readerIdCursor : entry.getValue()) {
                int readerId = readerIdCursor.value;
                String index = readerIndices.floorEntry(readerId).getValue();
                RelationName relationName = indicesToIdents.get(index);
                FetchSource fetchSource = fetchSources.get(relationName);
                if (fetchSource == null) {
                    continue;
                }
                streamersByReaderId.put(readerIdCursor.value, Symbols.streamerArray(fetchSource.references()));
            }
        }
        return streamersByReaderByNode;
    }

    public FetchSource getFetchSourceByReader(int readerId) {
        String index = readerIndices.floorEntry(readerId).getValue();
        RelationName relationName = indicesToIdents.get(index);
        assert relationName != null : "Must have a relationName for readerId=" + readerId;
        FetchSource fetchSource = fetchSources.get(relationName);
        assert fetchSource != null : "Must have a fetchSource for relationName=" + relationName;
        return fetchSource;
    }
}
