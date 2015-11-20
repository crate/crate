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
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.TableIdent;
import io.crate.planner.node.fetch.FetchSource;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FetchProjection extends Projection {

    private final int collectPhaseId;
    private final Map<TableIdent, FetchSource> fetchSources;
    private final List<Symbol> outputSymbols;
    private final Map<String, IntSet> nodeReaders;
    private final TreeMap<Integer, String> readerIndices;

    public FetchProjection(int collectPhaseId,
                           Map<TableIdent, FetchSource> fetchSources,
                           List<Symbol> outputSymbols,
                           Map<String, IntSet> nodeReaders,
                           TreeMap<Integer, String> readerIndices) {
        this.collectPhaseId = collectPhaseId;
        this.fetchSources = fetchSources;
        this.outputSymbols = outputSymbols;
        this.nodeReaders = nodeReaders;
        this.readerIndices = readerIndices;
    }

    public int collectPhaseId() {
        return collectPhaseId;
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
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }
}
