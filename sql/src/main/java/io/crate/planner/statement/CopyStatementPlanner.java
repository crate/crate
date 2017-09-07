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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import io.crate.analyze.CopyFromAnalyzedStatement;
import io.crate.analyze.CopyToAnalyzedStatement;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.collections.Lists2;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.projectors.TopN;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.consumer.ConsumerContext;
import io.crate.planner.consumer.FetchMode;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.FileUriCollectPhase;
import io.crate.planner.projection.MergeCountProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.SourceIndexWriterProjection;
import io.crate.planner.projection.WriterProjection;
import io.crate.planner.projection.builder.InputColumns;
import io.crate.planner.projection.builder.ProjectionBuilder;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class CopyStatementPlanner {

    private final ClusterService clusterService;

    public CopyStatementPlanner(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public Plan planCopyFrom(CopyFromAnalyzedStatement copyFrom, Planner.Context context) {
        /*
         * Create a plan that reads json-objects-lines from a file and then executes upsert requests to index the data
         */
        DocTableInfo table = copyFrom.table();
        String partitionIdent = copyFrom.partitionIdent();
        List<String> partitionedByNames = Collections.emptyList();
        List<BytesRef> partitionValues = Collections.emptyList();
        if (partitionIdent == null) {
            if (table.isPartitioned()) {
                partitionedByNames = Lists2.copyAndReplace(table.partitionedBy(), ColumnIdent::fqn);
            }
        } else {
            assert table.isPartitioned() : "table must be partitioned if partitionIdent is set";
            // partitionIdent is present -> possible to index raw source into concrete es index
            partitionValues = PartitionName.decodeIdent(partitionIdent);
        }

        // need to exclude _id columns; they're auto generated and won't be available in the files being imported
        ColumnIdent clusteredBy = table.clusteredBy();
        if (DocSysColumns.ID.equals(clusteredBy)) {
            clusteredBy = null;
        }
        List<Reference> primaryKeyRefs = table.primaryKey().stream()
            .filter(r -> !r.equals(DocSysColumns.ID))
            .map(table::getReference)
            .collect(Collectors.toList());

        List<Symbol> toCollect = getSymbolsRequiredForShardIdCalc(
            primaryKeyRefs,
            table.partitionedByColumns(),
            clusteredBy == null ? null : table.getReference(clusteredBy)
        );
        Reference rawOrDoc = rawOrDoc(table, partitionIdent);
        int rawOrDocIdx = toCollect.size();
        toCollect.add(rawOrDoc);
        String[] excludes = partitionedByNames.size() > 0
            ? partitionedByNames.toArray(new String[partitionedByNames.size()]) : null;

        InputColumns.Context inputColsContext = new InputColumns.Context(toCollect);
        Symbol clusteredByInputCol = null;
        if (clusteredBy != null) {
            clusteredByInputCol = InputColumns.create(table.getReference(clusteredBy), inputColsContext);
        }
        SourceIndexWriterProjection sourceIndexWriterProjection = new SourceIndexWriterProjection(
            table.ident(),
            partitionIdent,
            table.getReference(DocSysColumns.RAW),
            new InputColumn(rawOrDocIdx, rawOrDoc.valueType()),
            table.primaryKey(),
            InputColumns.create(table.partitionedByColumns(), inputColsContext),
            clusteredBy,
            copyFrom.settings(),
            null,
            excludes,
            InputColumns.create(primaryKeyRefs, inputColsContext),
            clusteredByInputCol,
            table.isPartitioned() // autoCreateIndices
        );
        List<Projection> projections = Collections.singletonList(sourceIndexWriterProjection);

        // if there are partitionValues (we've had a PARTITION clause in the statement)
        // we need to use the calculated partition values because the partition columns are likely NOT in the data being read
        // the partitionedBy-inputColumns created for the projection are still valid because the positions are not changed
        rewriteToCollectToUsePartitionValues(table.partitionedByColumns(), partitionValues, toCollect);

        DiscoveryNodes allNodes = clusterService.state().nodes();
        FileUriCollectPhase collectPhase = new FileUriCollectPhase(
            context.jobId(),
            context.nextExecutionPhaseId(),
            "copyFrom",
            getExecutionNodes(allNodes, copyFrom.settings().getAsInt("num_readers", allNodes.getSize()), copyFrom.nodePredicate()),
            copyFrom.uri(),
            toCollect,
            projections,
            copyFrom.settings().get("compression", null),
            copyFrom.settings().getAsBoolean("shared", null)
        );

        Collect collect = new Collect(collectPhase, TopN.NO_LIMIT, 0, 1, 1, null);
        return Merge.ensureOnHandler(collect, context, Collections.singletonList(MergeCountProjection.INSTANCE));
    }

    private static void rewriteToCollectToUsePartitionValues(List<Reference> partitionedByColumns,
                                                             List<BytesRef> partitionValues,
                                                             List<Symbol> toCollect) {
        for (int i = 0; i < partitionValues.size(); i++) {
            Reference partitionedByColumn = partitionedByColumns.get(i);
            int idx;
            if (partitionedByColumn instanceof GeneratedReference) {
                idx = toCollect.indexOf(((GeneratedReference) partitionedByColumn).generatedExpression());
            } else {
                idx = toCollect.indexOf(partitionedByColumn);
            }
            if (idx > -1) {
                toCollect.set(idx, Literal.of(partitionValues.get(i)));
            }
        }
    }

    /**
     * To generate the upsert request the following is required:
     *
     *  - tableIdent + partitionIdent / partitionValues
     *      -> to retrieve the indexName
     *
     *  - primaryKeys + clusteredBy  (+ indexName)
     *      -> to calculate the shardId
     */
    private static List<Symbol> getSymbolsRequiredForShardIdCalc(List<Reference> primaryKeyRefs,
                                                                 List<Reference> partitionedByRefs,
                                                                 @Nullable Reference clusteredBy) {
        HashSet<Symbol> toCollectUnique = new HashSet<>();
        primaryKeyRefs.forEach(r -> addWithRefDependencies(toCollectUnique, r));
        partitionedByRefs.forEach(r -> addWithRefDependencies(toCollectUnique, r));
        if (clusteredBy != null) {
            addWithRefDependencies(toCollectUnique, clusteredBy);
        }
        return new ArrayList<>(toCollectUnique);
    }

    private static void addWithRefDependencies(HashSet<Symbol> toCollectUnique, Reference ref) {
        if (ref instanceof GeneratedReference) {
            toCollectUnique.add(((GeneratedReference) ref).generatedExpression());
        } else {
            toCollectUnique.add(ref);
        }
    }

    /**
     * Return RAW or DOC Reference:
     *
     * Copy from has two "modes" on how the json-object-lines are processed:
     *
     * 1: non-partitioned tables or partitioned tables with partition ident --> import into single es index
     *    -> collect raw source and import as is
     *
     * 2: partitioned table without partition ident
     *    -> collect document and partition by values
     *    -> exclude partitioned by columns from document
     *    -> insert into es index (partition determined by partition by value)
     */
    private static Reference rawOrDoc(DocTableInfo table, String selectedPartitionIdent) {
        if (table.isPartitioned() && selectedPartitionIdent == null) {
            return table.getReference(DocSysColumns.DOC);
        }
        return table.getReference(DocSysColumns.RAW);
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

        ConsumerContext consumerContext = new ConsumerContext(context);
        consumerContext.setFetchMode(FetchMode.NEVER);
        Plan plan = context.planSubRelation(statement.subQueryRelation(), consumerContext);
        if (plan == null) {
            return null;
        }
        plan.addProjection(projection, null, null, null);
        return Merge.ensureOnHandler(plan, context, Collections.singletonList(MergeCountProjection.INSTANCE));
    }

    private static Collection<String> getExecutionNodes(DiscoveryNodes allNodes,
                                                        int maxNodes,
                                                        final Predicate<DiscoveryNode> nodeFilters) {
        int counter = maxNodes;
        final List<String> nodes = new ArrayList<>(allNodes.getSize());
        for (ObjectCursor<DiscoveryNode> cursor : allNodes.getDataNodes().values()) {
            if (nodeFilters.test(cursor.value) && counter-- > 0) {
                nodes.add(cursor.value.getId());
            }
        }
        return nodes;
    }
}
