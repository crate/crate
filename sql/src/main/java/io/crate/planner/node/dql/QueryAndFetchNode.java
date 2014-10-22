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

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.Constants;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.where.WhereClause;
import io.crate.metadata.Routing;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.PlanNodeTypeResolver;
import io.crate.planner.node.PlanVisitor;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
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
public class QueryAndFetchNode extends AbstractDQLPlanNode {

    private Optional<UUID> jobId = Optional.absent();
    private Routing routing;
    private List<Symbol> toCollect;
    private List<Symbol> outputSymbols;
    private WhereClause whereClause = WhereClause.MATCH_ALL;
    private RowGranularity maxRowgranularity = RowGranularity.CLUSTER;
    private List<String> downStreamNodes;
    private boolean isPartitioned = false;

    private List<Projection> collectorProjections = ImmutableList.of();

    protected List<DataType> intermediateOutputTypes = ImmutableList.of();

    private List<Symbol> orderBy;
    private int limit = Constants.DEFAULT_SELECT_LIMIT;
    private int offset = 0;

    private static final Boolean[] EMPTY_OBJ_BOOLEAN_ARR = new Boolean[0];
    private static final boolean[] EMPTY_VALUE_BOOLEAN_ARR = new boolean[0];

    private Boolean[] nullsFirst;
    private boolean[] reverseFlags;

    public QueryAndFetchNode() {
        super();
    }

    public QueryAndFetchNode(String id) {
        super(id);
    }

    public QueryAndFetchNode(@Nullable String id,
                             Routing routing,
                             List<Symbol> toCollect,
                             List<Symbol> outputSymbols,
                             @Nullable List<Symbol> orderBy,
                             @Nullable boolean[] reverseFlags,
                             @Nullable Boolean[] nullsFirst,
                             @Nullable Integer limit,
                             @Nullable Integer offset,
                             @Nullable List<Projection> collectorProjections,
                             @Nullable List<Projection> projections,
                             @Nullable WhereClause whereClause,
                             @Nullable RowGranularity rowGranularity,
                             @Nullable Boolean isPartitioned) {
        super(Objects.firstNonNull(id, "collect"));

        assert routing != null;
        assert toCollect != null;
        assert outputSymbols != null;

        this.routing = routing;
        this.toCollect = toCollect;
        this.outputSymbols = outputSymbols;
        this.whereClause = Objects.firstNonNull(whereClause, WhereClause.MATCH_ALL);

        this.limit = Objects.firstNonNull(limit, Constants.DEFAULT_SELECT_LIMIT);
        this.offset = Objects.firstNonNull(offset, 0);

        this.orderBy = Objects.firstNonNull(orderBy, ImmutableList.<Symbol>of());
        this.reverseFlags = Objects.firstNonNull(reverseFlags, EMPTY_VALUE_BOOLEAN_ARR);
        this.nullsFirst = Objects.firstNonNull(nullsFirst, EMPTY_OBJ_BOOLEAN_ARR);
        Preconditions.checkArgument(this.orderBy.size() == this.reverseFlags.length,
                "orderBy size doesn't match with reverseFlag length");

        if (rowGranularity != null) {
            maxRowGranularity(rowGranularity);
        }

        if (isPartitioned != null) {
            this.isPartitioned = isPartitioned;
        }
        this.collectorProjections(Objects.firstNonNull(collectorProjections, ImmutableList.<Projection>of()));
        this.projections(Objects.firstNonNull(projections, ImmutableList.<Projection>of()));
    }

    @Override
    public void configure() {
        extractOutputTypes();
    }

    /**
     * set the list of projection executed during merge
     */
    @Override
    public void projections(List<Projection> projections) {
        assert projections != null;
        this.projections = projections;
    }

    /**
     * get the list of projections executed during collection
     */
    public List<Projection> collectorProjections() {
        return collectorProjections;
    }

    /**
     * set the list of projections executed during collection
     */
    public void collectorProjections(List<Projection> collectorProjections) {
        assert collectorProjections != null;
        this.collectorProjections = collectorProjections;
    }

    /**
     * consider collectorProjections only when returning the final projection
     */
    @Override
    public Optional<Projection> finalProjection() {
        if (!collectorProjections.isEmpty()) {
            return Optional.of(collectorProjections.get(collectorProjections.size()-1));
        } else {
            return Optional.absent();
        }
    }

    /**
     * extract types of outputs of this node
     */
    private void extractOutputTypes() {
        List<DataType> toCollectTypes = PlanNodeTypeResolver.extractDataTypes(toCollect);
        if (collectorProjections.isEmpty() && projections.isEmpty()) {
            intermediateOutputTypes = toCollectTypes;
            outputTypes = PlanNodeTypeResolver.extractSymbolDataTypes(outputSymbols, intermediateOutputTypes);
        } else if (projections.isEmpty()) {
            intermediateOutputTypes = PlanNodeTypeResolver.extractDataTypes(collectorProjections, toCollectTypes);
            outputTypes = PlanNodeTypeResolver.extractDataTypes(collectorProjections, intermediateOutputTypes);
        } else {
            intermediateOutputTypes = PlanNodeTypeResolver.extractDataTypes(collectorProjections, toCollectTypes);
            outputTypes = PlanNodeTypeResolver.extractDataTypes(projections, intermediateOutputTypes);
        }
    }

    /**
     * This method returns true if downstreams are defined, which means that results of this collectForSelect
     * operation should be sent to other nodes instead of being returned directly.
     */
    public boolean hasDownstreams() {
        return downStreamNodes != null && downStreamNodes.size() > 0;
    }

    public List<String> downStreamNodes() {
        return downStreamNodes;
    }

    public void downStreamNodes(List<String> downStreamNodes) {
        this.downStreamNodes = downStreamNodes;
    }

    @Override
    public Set<String> executionNodes() {
        if (routing != null && routing.hasLocations()) {
            return routing.locations().keySet();
        } else {
            return ImmutableSet.of();
        }
    }

    public WhereClause whereClause() {
        return whereClause;
    }

    public void whereClause(WhereClause whereClause) {
        assert whereClause != null;
        this.whereClause = whereClause;
    }

    public Routing routing() {
        return routing;
    }

    public void routing(Routing routing) {
        this.routing = routing;
    }

    public List<Symbol> toCollect() {
        return toCollect;
    }

    public void toCollect(List<Symbol> toCollect) {
        assert toCollect != null;
        this.toCollect = toCollect;
    }

    public List<Symbol> outputSymbols() {
        return outputSymbols;
    }

    public void outputSymbols(List<Symbol> outputSymbols) {
        assert outputSymbols != null;
        this.outputSymbols = outputSymbols;
    }

    public void intermediateOutputTypes(List<DataType> outputTypes) {
        this.intermediateOutputTypes = outputTypes;
    }

    public List<DataType> intermediateOutputTypes() {
        return intermediateOutputTypes;
    }

    public boolean[] reverseFlags() {
        return reverseFlags;
    }

    public Boolean[] nullsFirst() {
        return nullsFirst;
    }

    public List<Symbol> orderBy() {
        return orderBy;
    }

    public int limit() {
        return limit;
    }

    public int offset() {
        return offset;
    }

    public boolean isRouted() {
        return routing != null && routing.hasLocations();
    }

    /**
     * Whether collectForSelect operates on a partitioned table.
     * Only used on {@link io.crate.operation.collect.HandlerSideDataCollectOperation},
     * so no serialization is needed.
     *
     * @return
     */
    public boolean isPartitioned() {
        return isPartitioned;
    }

    public void isPartitioned(boolean isPartitioned) {
        this.isPartitioned = isPartitioned;
    }

    public RowGranularity maxRowGranularity() {
        return maxRowgranularity;
    }

    public void maxRowGranularity(RowGranularity newRowGranularity) {
        if (maxRowgranularity.compareTo(newRowGranularity) < 0) {
            maxRowgranularity = newRowGranularity;
        }
    }

    public Optional<UUID> jobId() {
        return jobId;
    }

    public void jobId(@Nullable UUID jobId) {
        this.jobId = Optional.fromNullable(jobId);
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitQueryAndFetchNode(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        int numCols = in.readVInt();
        if (numCols > 0) {
            toCollect = new ArrayList<>(numCols);
            for (int i = 0; i < numCols; i++) {
                toCollect.add(Symbol.fromStream(in));
            }
        } else {
            toCollect = ImmutableList.of();
        }

        int numOuts = in.readVInt();
        if (numOuts > 0) {
            outputSymbols = new ArrayList<>(numOuts);
            for (int i = 0; i < numOuts; i++) {
                outputSymbols.add(Symbol.fromStream(in));
            }
        } else {
            outputSymbols = ImmutableList.of();
        }

        maxRowgranularity = RowGranularity.fromStream(in);

        if (in.readBoolean()) {
            routing = new Routing();
            routing.readFrom(in);
        }

        whereClause = new WhereClause(in);

        int numDownStreams = in.readVInt();
        downStreamNodes = new ArrayList<>(numDownStreams);
        for (int i = 0; i < numDownStreams; i++) {
            downStreamNodes.add(in.readString());
        }
        if (in.readBoolean()) {
            jobId = Optional.of(new UUID(in.readLong(), in.readLong()));
        }

        if (in.readBoolean()) {
            int numOrder = in.readVInt();
            orderBy = new ArrayList<>(numOrder);
            for (int i = 0; i < numOrder; i++) {
                orderBy().add(Symbol.fromStream(in));
            }
        }

        limit = in.readVInt();
        offset = in.readVInt();

        if (in.readBoolean()) {
            nullsFirst = new Boolean[in.readVInt()];
            for (int i = 0; i < nullsFirst.length; i++) {
                nullsFirst[i] = in.readOptionalBoolean();
            }
        }

        if (in.readBoolean()) {
            reverseFlags = new boolean[in.readVInt()];
            for (int i = 0; i < reverseFlags.length; i++) {
                reverseFlags[i] = in.readBoolean();
            }
        }

        int numIntermediateTypes = in.readVInt();
        intermediateOutputTypes = new ArrayList<>(numIntermediateTypes);
        for (int i = 0; i < numIntermediateTypes; i++) {
            intermediateOutputTypes.add(DataTypes.fromStream(in));
        }

        int numCollectorProjections = in.readVInt();
        collectorProjections = new ArrayList<>(numCollectorProjections);
        for (int i = 0; i < numCollectorProjections; i++) {
            collectorProjections.add(Projection.fromStream(in));
        }

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        int numCols = toCollect.size();
        out.writeVInt(numCols);
        for (int i = 0; i < numCols; i++) {
            Symbol.toStream(toCollect.get(i), out);
        }

        int numOut = outputSymbols.size();
        out.writeVInt(numOut);
        for (int i = 0; i < numOut; i++) {
            Symbol.toStream(outputSymbols().get(i), out);
        }

        RowGranularity.toStream(maxRowgranularity, out);

        if (routing != null) {
            out.writeBoolean(true);
            routing.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        whereClause.writeTo(out);

        if (hasDownstreams()) {
            out.writeVInt(downStreamNodes.size());
            for (String node : downStreamNodes) {
                out.writeString(node);
            }
        } else {
            out.writeVInt(0);
        }
        out.writeBoolean(jobId.isPresent());
        if (jobId.isPresent()) {
            out.writeLong(jobId.get().getMostSignificantBits());
            out.writeLong(jobId.get().getLeastSignificantBits());
        }

        if (orderBy != null) {
            out.writeBoolean(true);
            int numOrder = orderBy.size();
            out.writeVInt(numOrder);
            for (int i = 0; i < numOrder; i++) {
                Symbol.toStream(orderBy.get(i), out);
            }
        } else {
            out.writeBoolean(false);
        }

        out.writeVInt(limit);
        out.writeVInt(offset);

        if (nullsFirst != null) {
            out.writeBoolean(true);
            out.writeVInt(nullsFirst.length);
            for (int i = 0; i < nullsFirst.length; i++) {
                out.writeOptionalBoolean(nullsFirst[i]);
            }
        } else {
            out.writeBoolean(false);
        }

        if (reverseFlags != null) {
            out.writeBoolean(true);
            out.writeVInt(reverseFlags.length);
            for (int i = 0; i < reverseFlags.length; i++) {
                out.writeBoolean(reverseFlags[i]);
            }
        } else {
            out.writeBoolean(false);
        }

        int numIntermediateTypes = intermediateOutputTypes.size();
        out.writeVInt(numIntermediateTypes);
        for (int i = 0; i < numIntermediateTypes; i++) {
            DataTypes.toStream(intermediateOutputTypes.get(i), out);
        }

        out.writeVInt(collectorProjections.size());
        for (Projection collectorProjection : collectorProjections) {
            Projection.toStream(collectorProjection, out);
        }

    }

    /**
     * normalizes the symbols of this node with the given normalizer
     *
     * @param normalizer
     * @return a normalized node, if no changes occurred returns this
     */
    public QueryAndFetchNode normalize(EvaluatingNormalizer normalizer) {
        assert whereClause() != null;
        QueryAndFetchNode result = this;
        List<Symbol> newToCollect = normalizer.normalize(toCollect());
        boolean changed = newToCollect != toCollect();
        WhereClause newWhereClause = whereClause().normalize(normalizer);
        if (newWhereClause != whereClause()) {
            changed = changed || newWhereClause != whereClause();
        }
        if (changed) {
            result = new QueryAndFetchNode(id(),
                    routing,
                    newToCollect,
                    outputSymbols,
                    orderBy,
                    reverseFlags,
                    nullsFirst,
                    limit,
                    offset,
                    collectorProjections,
                    projections,
                    newWhereClause,
                    maxRowGranularity(),
                    isPartitioned);
            result.downStreamNodes = downStreamNodes;
            result.maxRowgranularity = maxRowgranularity;
            result.jobId = jobId;
        }
        return result;
    }

    public boolean isLimited() {
        return offset > 0 || limit != Constants.DEFAULT_SELECT_LIMIT;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("id", id())
                .add("collectorProjections", collectorProjections)
                .add("projections", projections)
                .add("intermediateOutputTypes", intermediateOutputTypes)
                .add("outputTypes", outputTypes)
                .toString();
    }
}
