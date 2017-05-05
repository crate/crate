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

package io.crate.planner.fetch;

import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.TableIdent;
import io.crate.planner.Planner;
import io.crate.planner.ReaderAllocations;
import io.crate.planner.node.fetch.FetchPhase;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.planner.projection.FetchProjection;
import io.crate.planner.projection.builder.InputColumns;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

public final class FetchPushDown {

    private FetchPushDown() {
    }

    @Nullable
    public static Builder<MultiSourceSelect> pushDown(MultiSourceSelect mss) {
        if (mss.canBeFetched().isEmpty()) {
            return null;
        }

        MultiSourceFetchPushDown pd = new MultiSourceFetchPushDown(mss);
        pd.process();
        LinkedHashSet<Reference> fetchRefs = new LinkedHashSet<>();
        for (FetchSource fetchSource : pd.fetchSources().values()) {
            fetchRefs.addAll(fetchSource.references());
        }
        return new Builder<>(
            fetchRefs,
            pd.fetchSources(),
            InputColumns.create(pd.remainingOutputs(), new InputColumns.Context(mss.querySpec().outputs())),
            mss
        );
    }

    public static class PhaseAndProjection {

        public final FetchPhase phase;
        public final FetchProjection projection;

        private PhaseAndProjection(FetchPhase phase, FetchProjection projection) {
            this.phase = phase;
            this.projection = projection;
        }
    }

    public static class Builder<T extends QueriedRelation> {

        private final Collection<Reference> fetchRefs;
        private final Map<TableIdent, FetchSource> fetchSourceByTable;
        private final List<Symbol> outputs;
        private final T replacedRelation;

        public Builder(Collection<Reference> fetchRefs,
                       Map<TableIdent, FetchSource> fetchSourceByTable,
                       List<Symbol> outputs,
                       T replacedRelation) {
            this.fetchRefs = fetchRefs;
            this.fetchSourceByTable = fetchSourceByTable;
            this.outputs = outputs;
            this.replacedRelation = replacedRelation;
        }

        /**
         *  Builds the {@link FetchPhase} and {@link FetchProjection}.
         *  They can only be build after the plans are processed and all shards and readers are allocated.
         */
        public PhaseAndProjection build(Planner.Context plannerContext) {
            ReaderAllocations readerAllocations = plannerContext.buildReaderAllocations();

            FetchPhase fetchPhase = new FetchPhase(
                plannerContext.nextExecutionPhaseId(),
                readerAllocations.nodeReaders().keySet(),
                readerAllocations.bases(),
                readerAllocations.tableIndices(),
                fetchRefs
            );
            FetchProjection fetchProjection = new FetchProjection(
                fetchPhase.phaseId(),
                plannerContext.fetchSize(),
                fetchSourceByTable,
                outputs,
                readerAllocations.nodeReaders(),
                readerAllocations.indices(),
                readerAllocations.indicesToIdents());

            return new PhaseAndProjection(fetchPhase, fetchProjection);
        }

        public T replacedRelation() {
            return replacedRelation;
        }
    }
}
