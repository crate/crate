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

package io.crate.execution.dsl.phases;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.UnaryOperator;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.common.collections.Lists;
import io.crate.data.Paging;
import io.crate.execution.dsl.projection.Projection;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.planner.distribution.DistributionInfo;

/**
 * A plan node which collects data.
 */
public class RoutedCollectPhase extends AbstractProjectionsPhase implements CollectPhase {

    private final Routing routing;
    private final List<Symbol> toCollect;
    private final RowGranularity maxRowGranularity;
    private final Symbol where;

    private DistributionInfo distributionInfo;

    @Nullable
    private Integer nodePageSizeHint = null;

    @Nullable
    private OrderBy orderBy = null;

    public RoutedCollectPhase(UUID jobId,
                              int executionNodeId,
                              String name,
                              Routing routing,
                              RowGranularity maxRowGranularity,
                              List<Symbol> toCollect,
                              Collection<? extends Projection> projections,
                              Symbol where,
                              DistributionInfo distributionInfo) {
        super(jobId, executionNodeId, name, projections);
        assert toCollect.stream().noneMatch(
            st -> SymbolVisitors.any(s -> s instanceof ScopedSymbol || s instanceof SelectSymbol, st))
            : "toCollect must not contain any fields or selectSymbols: " + toCollect;
        assert !SymbolVisitors.any(s -> s instanceof ScopedSymbol || s instanceof SelectSymbol, where)
            : "whereClause must not contain any fields or selectSymbols: " + where;
        assert routing != null : "routing must not be null";

        this.where = where;
        this.routing = routing;
        this.maxRowGranularity = maxRowGranularity;
        this.toCollect = toCollect;
        this.distributionInfo = distributionInfo;
        this.outputTypes = extractOutputTypes(toCollect, this.projections);
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
    @Nullable
    public Integer nodePageSizeHint() {
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
        assert orderBy == null || orderBy.orderBySymbols().stream().noneMatch(st -> SymbolVisitors.any(s -> s instanceof ScopedSymbol, st))
            : "orderBy must not contain any fields: " + orderBy.orderBySymbols();
        this.orderBy = orderBy;
    }

    public Symbol where() {
        return where;
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

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitRoutedCollectPhase(this, context);
    }

    public RoutedCollectPhase(StreamInput in) throws IOException {
        super(in);
        distributionInfo = new DistributionInfo(in);

        toCollect = Symbols.listFromStream(in);
        maxRowGranularity = RowGranularity.fromStream(in);

        routing = new Routing(in);

        where = Symbols.fromStream(in);

        nodePageSizeHint = in.readOptionalVInt();

        orderBy = in.readOptionalWriteable(OrderBy::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        distributionInfo.writeTo(out);

        Symbols.toStream(toCollect, out);
        RowGranularity.toStream(maxRowGranularity, out);

        routing.writeTo(out);
        Symbols.toStream(where, out);

        out.writeOptionalVInt(nodePageSizeHint);
        out.writeOptionalWriteable(orderBy);
    }

    /**
     * normalizes the symbols of this node with the given normalizer
     *
     * @return a normalized node, if no changes occurred returns this
     */
    public RoutedCollectPhase normalize(EvaluatingNormalizer normalizer, @NotNull TransactionContext txnCtx) {
        RoutedCollectPhase result = this;
        UnaryOperator<Symbol> normalize = s -> normalizer.normalize(s, txnCtx);
        List<Symbol> newToCollect = Lists.map(toCollect, normalize);
        boolean changed = !newToCollect.equals(toCollect);
        Symbol newWhereClause = normalizer.normalize(where, txnCtx);
        OrderBy orderBy = this.orderBy;
        if (orderBy != null) {
            orderBy = orderBy.map(normalize);
        }
        changed = changed || newWhereClause != where || orderBy != this.orderBy;
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
                distributionInfo
            );
            result.nodePageSizeHint(nodePageSizeHint);
            result.orderBy(orderBy);
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RoutedCollectPhase that = (RoutedCollectPhase) o;
        return Objects.equals(routing, that.routing) &&
               Objects.equals(toCollect, that.toCollect) &&
               maxRowGranularity == that.maxRowGranularity &&
               Objects.equals(where, that.where) &&
               Objects.equals(distributionInfo, that.distributionInfo) &&
               Objects.equals(nodePageSizeHint, that.nodePageSizeHint) &&
               Objects.equals(orderBy, that.orderBy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(), routing, toCollect, maxRowGranularity, where, distributionInfo, nodePageSizeHint, orderBy);
    }
}
