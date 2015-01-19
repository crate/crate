package io.crate.planner.node.dml;


import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.RelationVisitor;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.Path;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Field;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.List;

public class QueryAndFetchNode implements PlannedAnalyzedRelation, PlanNode {

    private final CollectNode collectNode;
    @Nullable
    private final MergeNode localMergeNode;

    public QueryAndFetchNode(CollectNode collectNode, @Nullable MergeNode localMergeNode){
        this.collectNode = collectNode;
        this.localMergeNode = localMergeNode;
    }

    public QueryAndFetchNode(CollectNode collectNode) {
        this(collectNode, null);
    }

    @Override
    public <C, R> R accept(RelationVisitor<C, R> visitor, C context) {
        return visitor.visitPlanedAnalyzedRelation(this, context);
    }

    @Nullable
    @Override
    public Field getField(Path path) {
        throw new UnsupportedOperationException("getField is not supported");
    }

    @Override
    public Field getWritableField(Path path) throws UnsupportedOperationException, ColumnUnknownException {
        throw new UnsupportedOperationException("getWritableField is not supported");
    }

    @Override
    public List<Field> fields() {
        throw new UnsupportedOperationException("fields is not supported");
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitQueryAndFetchNode(this, context);
    }

    @Override
    public List<DataType> outputTypes() {
        return localMergeNode.outputTypes();
    }

    @Override
    public void outputTypes(List<DataType> outputTypes) {
        throw new UnsupportedOperationException("set outputTypes is not supported");
    }

    @Override
    public void addProjection(Projection projection) {
        collectNode.addProjection(projection);
    }

    public CollectNode collectNode() {
        return collectNode;
    }

    @Nullable
    public MergeNode localMergeNode(){
        return localMergeNode;
    }
}
