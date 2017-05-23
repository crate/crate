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

package io.crate.planner.projection;

import com.carrotsearch.hppc.IntSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.symbol.Symbol;
import io.crate.collections.Lists2;
import io.crate.metadata.TableIdent;
import io.crate.operation.Paging;
import io.crate.planner.ExplainLeaf;
import io.crate.planner.node.fetch.FetchSource;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

public class FetchProjection extends Projection {

    private static final int HEAP_IN_MB = (int) JvmInfo.jvmInfo().getConfiguredMaxHeapSize() / (1024 * 1024);
    private static final int MAX_FETCH_SIZE = maxFetchSize(HEAP_IN_MB);

    private final int collectPhaseId;
    private final int fetchSize;
    private final Map<TableIdent, FetchSource> fetchSources;
    private final List<Symbol> outputSymbols;
    private final Map<String, IntSet> nodeReaders;
    private final TreeMap<Integer, String> readerIndices;
    private final Map<String, TableIdent> indicesToIdents;

    public FetchProjection(int collectPhaseId,
                           int suppliedFetchSize,
                           Map<TableIdent, FetchSource> fetchSources,
                           List<Symbol> outputSymbols,
                           Map<String, IntSet> nodeReaders,
                           TreeMap<Integer, String> readerIndices,
                           Map<String, TableIdent> indicesToIdents) {
        this.collectPhaseId = collectPhaseId;
        this.fetchSources = fetchSources;
        this.outputSymbols = outputSymbols;
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

    public int collectPhaseId() {
        return collectPhaseId;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public Map<TableIdent, FetchSource> fetchSources() {
        return fetchSources;
    }

    public List<Symbol> outputSymbols() {
        return outputSymbols;
    }

    public Map<String, IntSet> nodeReaders() {
        return nodeReaders;
    }

    public TreeMap<Integer, String> readerIndices() {
        return readerIndices;
    }

    public Map<String, TableIdent> indicesToIdents() {
        return indicesToIdents;
    }

    @Override
    public void replaceSymbols(Function<Symbol, Symbol> replaceFunction) {
        Lists2.replaceItems(outputSymbols, replaceFunction);
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
        return collectPhaseId == that.collectPhaseId;
    }

    @Override
    public int hashCode() {
        return collectPhaseId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("writeTo is not supported for " +
                                                FetchProjection.class.getSimpleName());
    }

    @Override
    public Map<String, Object> mapRepresentation() {
        return ImmutableMap.of(
            "type", "Fetch",
            "outputs", ExplainLeaf.printList(outputSymbols),
            "fetchSize", fetchSize
        );
    }
}
