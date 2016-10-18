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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.UnionSelect;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.TableIdent;
import io.crate.planner.Planner;
import io.crate.planner.node.fetch.FetchPhase;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.planner.projection.FetchProjection;

import javax.annotation.Nullable;
import java.util.*;

public class FetchPushDown {

    private static FetchPushDown.Visitor VISITOR = new FetchPushDown.Visitor();

    /**
     * Returns a {@link Builder} if some {@link Reference} can be fetched later on.
     * If not, null will be returned.
     *
     * Use that builder to build a {@link FetchPhase} and {@link FetchProjection} after the plans were processed,
     * see {@link Builder#build(Planner.Context)}.
     */
    @Nullable
    public static Builder pushDown(AnalyzedRelation relation) {
        VisitorContext context = new VisitorContext();
        VISITOR.process(relation, context);

        if (context.fetchSources.isEmpty()) {
            // no fetch required
            return null;
        }

        return new Builder(context);
    }

    public static class PhaseAndProjection {

        public final FetchPhase phase;
        public final FetchProjection projection;

        private PhaseAndProjection(FetchPhase phase, FetchProjection projection) {
            this.phase = phase;
            this.projection = projection;
        }
    }

    public static class Builder {

        private final VisitorContext visitorContext;

        private Builder(VisitorContext visitorContext) {
            this.visitorContext = visitorContext;
        }

        /**
         *  Builds the {@link FetchPhase} and {@link FetchProjection}.
         *  They can only be build after the plans are processed and all shards and readers are allocated.
         */
        public PhaseAndProjection build(Planner.Context plannerContext) {
            Planner.Context.ReaderAllocations readerAllocations = plannerContext.buildReaderAllocations();

            FetchPhase fetchPhase = new FetchPhase(
                plannerContext.nextExecutionPhaseId(),
                readerAllocations.nodeReaders().keySet(),
                readerAllocations.bases(),
                readerAllocations.tableIndices(),
                visitorContext.fetchRefs
            );
            FetchProjection fetchProjection = new FetchProjection(
                fetchPhase.phaseId(),
                plannerContext.fetchSize(),
                visitorContext.fetchSources,
                visitorContext.outputsPerRelation,
                readerAllocations.nodeReaders(),
                readerAllocations.indices(),
                readerAllocations.indicesToIdents());

            return new PhaseAndProjection(fetchPhase, fetchProjection);
        }

        @Nullable
        public AnalyzedRelation replacedRelation() {
            return visitorContext.replacedRelation;
        }
    }

    private static class VisitorContext {

        Map<TableIdent, FetchSource> fetchSources = new HashMap<>();
        LinkedHashSet<Reference> fetchRefs = new LinkedHashSet<>();
        List<List<Symbol>> outputsPerRelation = new ArrayList<>();
        QueriedRelation replacedRelation;
    }

    private static class Visitor extends AnalyzedRelationVisitor<VisitorContext, Void> {

        @Override
        public Void visitUnionSelect(UnionSelect unionSelect, VisitorContext context) {
            for (int i = 0; i < unionSelect.relations().size(); i++) {
                process(unionSelect.relations().get(i), context);
                if (context.replacedRelation != null) {
                    unionSelect.relations().set(i, context.replacedRelation);
                }
            }
            return null;
        }

        @Override
        public Void visitQueriedTable(QueriedTable table, VisitorContext context) {
            context.replacedRelation = null;
            return null;
        }

        @Override
        public Void visitQueriedDocTable(QueriedDocTable relation, VisitorContext context) {
            QueriedDocTableFetchPushDown docTableFetchPushDown = new QueriedDocTableFetchPushDown(relation);

            QueriedDocTable subRelation = docTableFetchPushDown.pushDown();
            if (subRelation == null) {
                // no fetch required
                context.replacedRelation = null;
                return null;
            }
            context.replacedRelation = subRelation;
            Collection<Reference> fetchRefs = docTableFetchPushDown.fetchRefs();
            context.fetchRefs.addAll(fetchRefs);
            context.outputsPerRelation.add(relation.querySpec().outputs());

            TableIdent tableIdent = relation.tableRelation().tableInfo().ident();
            FetchSource fetchSource = context.fetchSources.get(tableIdent);

            if (fetchSource == null) {
                context.fetchSources.put(tableIdent,
                    new FetchSource(relation.tableRelation().tableInfo().partitionedByColumns(),
                        ImmutableList.of(docTableFetchPushDown.docIdCol()), fetchRefs));
            } else {
                // merge fetch references without duplicates
                LinkedHashSet<Reference> mergedFetchRefs = new LinkedHashSet<>();
                mergedFetchRefs.addAll(fetchSource.references());
                mergedFetchRefs.addAll(fetchRefs);
                context.fetchSources.put(tableIdent,
                    new FetchSource(relation.tableRelation().tableInfo().partitionedByColumns(),
                        ImmutableList.of(docTableFetchPushDown.docIdCol()), mergedFetchRefs));
            }

            return null;
        }

        @Override
        public Void visitMultiSourceSelect(MultiSourceSelect multiSourceSelect, VisitorContext context) {
            if (multiSourceSelect.canBeFetched().isEmpty()) {
                context.replacedRelation = null;
                return null;
            }

            MultiSourceFetchPushDown pd = new MultiSourceFetchPushDown(multiSourceSelect);
            pd.process();
            context.replacedRelation = multiSourceSelect;
            context.fetchSources.putAll(pd.fetchSources());
            context.outputsPerRelation.add(pd.remainingOutputs());

            for (Map.Entry<TableIdent, FetchSource> entry : pd.fetchSources().entrySet()) {
                context.fetchRefs.addAll(entry.getValue().references());
            }

            return null;
        }
    }
}
