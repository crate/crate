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

package io.crate.planner.statement;

import com.carrotsearch.hppc.procedures.ObjectProcedure;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.analyze.CopyFromAnalyzedStatement;
import io.crate.analyze.CopyToAnalyzedStatement;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.consumer.ConsumerContext;
import io.crate.planner.node.dml.CopyTo;
import io.crate.planner.node.dql.CollectAndMerge;
import io.crate.planner.node.dql.FileUriCollectPhase;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.projection.MergeCountProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.SourceIndexWriterProjection;
import io.crate.planner.projection.WriterProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CopyStatementPlanner {

    private final ClusterService clusterService;

    public CopyStatementPlanner(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public Plan planCopyFrom(CopyFromAnalyzedStatement analysis, Planner.Context context) {
        /**
         * copy from has two "modes":
         *
         * 1: non-partitioned tables or partitioned tables with partition ident --> import into single es index
         *    -> collect raw source and import as is
         *
         * 2: partitioned table without partition ident
         *    -> collect document and partition by values
         *    -> exclude partitioned by columns from document
         *    -> insert into es index (partition determined by partition by value)
         */

        DocTableInfo table = analysis.table();
        int clusteredByPrimaryKeyIdx = table.primaryKey().indexOf(analysis.table().clusteredBy());
        List<String> partitionedByNames;
        String partitionIdent = null;

        List<BytesRef> partitionValues;
        if (analysis.partitionIdent() == null) {

            if (table.isPartitioned()) {
                partitionedByNames = Lists.newArrayList(
                    Lists.transform(table.partitionedBy(), ColumnIdent.GET_FQN_NAME_FUNCTION));
            } else {
                partitionedByNames = Collections.emptyList();
            }
            partitionValues = ImmutableList.of();
        } else {
            assert table.isPartitioned() : "table must be partitioned if partitionIdent is set";
            // partitionIdent is present -> possible to index raw source into concrete es index

            partitionValues = PartitionName.decodeIdent(analysis.partitionIdent());
            partitionIdent = analysis.partitionIdent();
            partitionedByNames = Collections.emptyList();
        }

        SourceIndexWriterProjection sourceIndexWriterProjection = new SourceIndexWriterProjection(
            table.ident(),
            partitionIdent,
            table.getReference(DocSysColumns.RAW),
            table.primaryKey(),
            table.partitionedBy(),
            partitionValues,
            table.clusteredBy(),
            clusteredByPrimaryKeyIdx,
            analysis.settings(),
            null,
            partitionedByNames.size() >
            0 ? partitionedByNames.toArray(new String[partitionedByNames.size()]) : null,
            table.isPartitioned() // autoCreateIndices
        );
        List<Projection> projections = Collections.<Projection>singletonList(sourceIndexWriterProjection);
        partitionedByNames.removeAll(Lists.transform(table.primaryKey(), ColumnIdent.GET_FQN_NAME_FUNCTION));
        int referencesSize = table.primaryKey().size() + partitionedByNames.size() + 1;
        referencesSize = clusteredByPrimaryKeyIdx == -1 ? referencesSize + 1 : referencesSize;

        List<Symbol> toCollect = new ArrayList<>(referencesSize);
        // add primaryKey columns
        for (ColumnIdent primaryKey : table.primaryKey()) {
            toCollect.add(table.getReference(primaryKey));
        }

        // add partitioned columns (if not part of primaryKey)
        Set<Reference> referencedReferences = new HashSet<>();
        for (String partitionedColumn : partitionedByNames) {
            Reference reference = table.getReference(ColumnIdent.fromPath(partitionedColumn));
            Symbol symbol;
            if (reference instanceof GeneratedReference) {
                symbol = ((GeneratedReference) reference).generatedExpression();
                referencedReferences.addAll(((GeneratedReference) reference).referencedReferences());
            } else {
                symbol = reference;
            }
            toCollect.add(symbol);
        }
        // add clusteredBy column (if not part of primaryKey)
        if (clusteredByPrimaryKeyIdx == -1 && table.clusteredBy() != null &&
            !DocSysColumns.ID.equals(table.clusteredBy())) {
            toCollect.add(table.getReference(table.clusteredBy()));
        }
        // add _raw or _doc
        if (table.isPartitioned() && analysis.partitionIdent() == null) {
            toCollect.add(table.getReference(DocSysColumns.DOC));
        } else {
            toCollect.add(table.getReference(DocSysColumns.RAW));
        }

        // add columns referenced by generated columns which are used as partitioned by column
        for (Reference reference : referencedReferences) {
            if (!toCollect.contains(reference)) {
                toCollect.add(reference);
            }
        }

        DiscoveryNodes allNodes = clusterService.state().nodes();
        FileUriCollectPhase collectPhase = new FileUriCollectPhase(
            context.jobId(),
            context.nextExecutionPhaseId(),
            "copyFrom",
            getExecutionNodes(allNodes, analysis.settings().getAsInt("num_readers", allNodes.getSize()), analysis.nodePredicate()),
            analysis.uri(),
            toCollect,
            projections,
            analysis.settings().get("compression", null),
            analysis.settings().getAsBoolean("shared", null)
        );

        return new CollectAndMerge(collectPhase, MergePhase.localMerge(
            context.jobId(),
            context.nextExecutionPhaseId(),
            ImmutableList.<Projection>of(MergeCountProjection.INSTANCE),
            collectPhase.nodeIds().size(),
            collectPhase.outputTypes()));
    }

    public Plan planCopyTo(CopyToAnalyzedStatement statement, Planner.Context context) {
        WriterProjection.OutputFormat outputFormat = statement.outputFormat();
        if (outputFormat == null) {
            outputFormat = statement.columnsDefined() ?
                WriterProjection.OutputFormat.JSON_ARRAY : WriterProjection.OutputFormat.JSON_OBJECT;
        }

        WriterProjection projection = ProjectionBuilder.writerProjection(
            statement.subQueryRelation().querySpec().outputs(),
            statement.uri(),
            statement.compressionType(),
            statement.overwrites(),
            statement.outputNames(),
            outputFormat);

        Plan plan = context.planSubRelation(statement.subQueryRelation(), new ConsumerContext(statement, context));
        if (plan == null) {
            return null;
        }

        plan.addProjection(projection);

        MergePhase mergePhase = MergePhase.localMerge(
            context.jobId(),
            context.nextExecutionPhaseId(),
            ImmutableList.<Projection>of(MergeCountProjection.INSTANCE),
            plan.resultDescription().nodeIds().size(),
            Symbols.extractTypes(projection.outputs()));

        return new CopyTo(context.jobId(), plan, mergePhase);
    }

    private static Collection<String> getExecutionNodes(DiscoveryNodes allNodes,
                                                        int maxNodes,
                                                        final Predicate<DiscoveryNode> nodeFilters) {
        final AtomicInteger counter = new AtomicInteger(maxNodes);
        final List<String> nodes = new ArrayList<>(allNodes.size());
        allNodes.dataNodes().values().forEach(new ObjectProcedure<DiscoveryNode>() {
            @Override
            public void apply(DiscoveryNode value) {
                if (nodeFilters.apply(value) && counter.getAndDecrement() > 0) {
                    nodes.add(value.id());
                }
            }
        });
        return nodes;
    }
}
