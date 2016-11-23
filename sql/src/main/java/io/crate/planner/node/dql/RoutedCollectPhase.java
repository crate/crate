/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner.node.dql;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.collections.Lists2;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.Paging;
import io.crate.planner.Planner;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ExecutionPhaseVisitor;
import io.crate.planner.projection.Projection;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * A plan node which collects data.
 */
public class RoutedCollectPhase extends AbstractProjectionsPhase implements CollectPhase {

    public static final ExecutionPhaseFactory<RoutedCollectPhase> FACTORY = RoutedCollectPhase::new;

    private byte relationId = 0;
    private Routing routing;
    private List<Symbol> toCollect;
    private DistributionInfo distributionInfo;
    private WhereClause whereClause = WhereClause.MATCH_ALL;
    private RowGranularity maxRowGranularity = RowGranularity.CLUSTER;

    private boolean isPartitioned = false;

    @Nullable
    private Integer nodePageSizeHint = null;

    @Nullable
    private OrderBy orderBy = null;

    protected RoutedCollectPhase() {
        super();
    }

    public RoutedCollectPhase(UUID jobId,
                              int executionNodeId,
                              String name,
                              Routing routing,
                              RowGranularity maxRowGranularity,
                              List<Symbol> toCollect,
                              List<Projection> projections,
                              WhereClause whereClause,
                              DistributionInfo distributionInfo,
                              byte relationId) {
        super(jobId, executionNodeId, name, projections);
        this.whereClause = whereClause;
        this.routing = routing;
        this.maxRowGranularity = maxRowGranularity;
        this.toCollect = toCollect;
        this.distributionInfo = distributionInfo;
        this.outputTypes = extractOutputTypes(toCollect, projections);
        this.relationId = relationId;
    }

    @Override
    public void replaceSymbols(Function<Symbol, Symbol> replaceFunction) {
        super.replaceSymbols(replaceFunction);
        if (whereClause.hasQuery()) {
            Symbol query = whereClause.query();
            Symbol newQuery = replaceFunction.apply(query);
            if (query != newQuery) {
                whereClause = new WhereClause(newQuery, whereClause.docKeys().orNull(), whereClause.partitions());
            }
        }
        Lists2.replaceItems(toCollect, replaceFunction);
        if (orderBy != null) {
            orderBy.replace(replaceFunction);
        }
    }

    @Override
    public Type type() {
        return Type.COLLECT;
    }

    /**
     * @return a set of node ids where this collect operation is executed,
     */
    @Override
    public Set<String> nodeIds() {
        if (routing == null) {
            return ImmutableSet.of();
        }
        return routing.nodes();
    }

    @Override
    public DistributionInfo distributionInfo() {
        return distributionInfo;
    }

    @Override
    public void distributionInfo(DistributionInfo distributionInfo) {
        this.distributionInfo = distributionInfo;
    }

    /**
     * This is the amount of rows a node *probably* has to provide in order to have enough rows to satisfy the query limit.
     * </p>
     * <p>
     * E.g. in a query like <pre>select * from t limit 1000</pre> in a 2 node cluster each node probably only has to return 500 rows.
     * </p>
     */
    public
    @Nullable
    Integer nodePageSizeHint() {
        return nodePageSizeHint;
    }

    /**
     * <p><
     * set the nodePageSizeHint
     * <p>
     * See {@link #nodePageSizeHint()}
     * <p>
     * NOTE: if the collectPhase provides a directResult (instead of push result) the nodePageSizeHint has to be set
     * to the query hard-limit because there is no way to fetch more rows.
     */
    public void nodePageSizeHint(Integer nodePageSizeHint) {
        this.nodePageSizeHint = nodePageSizeHint;
    }


    /**
     * Similar to {@link #nodePageSizeHint(Integer)} in that it sets the nodePageSizeHint, but the given
     * pageSize is the total pageSize.
     */
    public void pageSizeHint(Integer pageSize) {
        nodePageSizeHint(Paging.getWeightedPageSize(pageSize, 1.0d / Math.max(1, nodeIds().size())));
    }

    /**
     * returns the shardQueueSize for a given node. <br />
     * This depends on the {@link #nodePageSizeHint()} and the number of shards that are on the given node.
     * <p>
     * <p>
     * E.g. Given 10 shards in total in an uneven distribution (8 and 2) and a nodePageSize of 10000 <br />
     * The shardQueueSize on the node with 2 shards is ~5000 (+ overhead). <br />
     * On the oder node (8 shards) the shardQueueSize will be ~1250 (+ overhead)
     * </p>
     * <p>
     * This is because numShardsOnNode * shardQueueSize should be >= nodePageSizeHint
     * </p>
     *
     * @param nodeId the node for which to get the shardQueueSize
     */
    public int shardQueueSize(String nodeId) {
        return Paging.getWeightedPageSize(nodePageSizeHint, 1.0d / Math.max(1, routing.numShards(nodeId)));
    }

    @Nullable
    public OrderBy orderBy() {
        return orderBy;
    }

    public void orderBy(@Nullable OrderBy orderBy) {
        this.orderBy = orderBy;
    }

    public WhereClause whereClause() {
        return whereClause;
    }

    public Routing routing() {
        return routing;
    }

    public List<Symbol> toCollect() {
        return toCollect;
    }

    public boolean isRouted() {
        return routing != null && routing.hasLocations();
    }

    public RowGranularity maxRowGranularity() {
        return maxRowGranularity;
    }

    public byte relationId() {
        return relationId;
    }

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitRoutedCollectPhase(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        distributionInfo = DistributionInfo.fromStream(in);

        toCollect = Symbols.listFromStream(in);
        maxRowGranularity = RowGranularity.fromStream(in);

        if (in.readBoolean()) {
            routing = Routing.fromStream(in);
        }

        whereClause = new WhereClause(in);

        if (in.readBoolean()) {
            nodePageSizeHint = in.readVInt();
        }

        if (in.readBoolean()) {
            orderBy = OrderBy.fromStream(in);
        }
        isPartitioned = in.readBoolean();

        relationId = in.readByte();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        distributionInfo.writeTo(out);

        Symbols.toStream(toCollect, out);
        RowGranularity.toStream(maxRowGranularity, out);

        if (routing != null) {
            out.writeBoolean(true);
            routing.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        whereClause.writeTo(out);

        if (nodePageSizeHint != null) {
            out.writeBoolean(true);
            out.writeVInt(nodePageSizeHint);
        } else {
            out.writeBoolean(false);
        }
        if (orderBy != null) {
            out.writeBoolean(true);
            OrderBy.toStream(orderBy, out);
        } else {
            out.writeBoolean(false);
        }
        out.writeBoolean(isPartitioned);
        out.writeByte(relationId);
    }

    /**
     * normalizes the symbols of this node with the given normalizer
     *
     * @return a normalized node, if no changes occurred returns this
     */
    public RoutedCollectPhase normalize(EvaluatingNormalizer normalizer, TransactionContext transactionContext) {
        assert whereClause() != null : "whereClause must not be null";
        RoutedCollectPhase result = this;
        List<Symbol> newToCollect = normalizer.normalize(toCollect(), transactionContext);
        boolean changed = newToCollect != toCollect();
        WhereClause newWhereClause = whereClause().normalize(normalizer, transactionContext);
        OrderBy orderBy = this.orderBy;
        if (orderBy != null) {
            List<Symbol> orderBySymbols = orderBy.orderBySymbols();
            List<Symbol> normalizedOrderBy = normalizer.normalize(orderBySymbols, transactionContext);
            changed = changed || orderBySymbols != normalizedOrderBy;
            orderBy = new OrderBy(normalizedOrderBy, this.orderBy.reverseFlags(), this.orderBy.nullsFirst());
        }
        changed = changed || newWhereClause != whereClause();
        if (changed) {
            result = new RoutedCollectPhase(
                jobId(),
                phaseId(),
                name(),
                routing,
                maxRowGranularity,
                newToCollect,
                projections,
                newWhereClause,
                distributionInfo,
                relationId
            );
            result.orderBy(orderBy);
        }
        return result;
    }

    public static RoutedCollectPhase forQueriedTable(Planner.Context plannerContext,
                                                     QueriedTableRelation table,
                                                     List<Symbol> toCollect,
                                                     List<Projection> projections,
                                                     byte relationId) {
        TableInfo tableInfo = table.tableRelation().tableInfo();
        WhereClause where = table.querySpec().where();
        if (table.tableRelation() instanceof TableFunctionRelation) {
            TableFunctionRelation tableFunctionRelation = (TableFunctionRelation) table.tableRelation();
            List<Symbol> args = new ArrayList<>();
            args.addAll(tableFunctionRelation.arguments());
            plannerContext.normalizer().normalizeInplace(args, plannerContext.transactionContext());
            return new TableFunctionCollectPhase(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                plannerContext.allocateRouting(tableInfo, where, null),
                tableFunctionRelation.functionName(),
                args,
                projections,
                toCollect,
                where,
                relationId
            );
        }
        return new RoutedCollectPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "collect",
            plannerContext.allocateRouting(tableInfo, where, null),
            tableInfo.rowGranularity(),
            toCollect,
            projections,
            where,
            DistributionInfo.DEFAULT_BROADCAST,
            relationId
        );
    }
}
