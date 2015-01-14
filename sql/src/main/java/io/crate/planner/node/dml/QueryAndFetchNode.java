package io.crate.planner.node.dml;


import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.RelationVisitor;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.Path;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.PlanVisitor;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.symbol.Field;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.List;

public class QueryAndFetchNode implements PlannedAnalyzedRelation, PlanNode {

    private final CollectNode collectNode;
    private final MergeNode localMergeNode;

    public QueryAndFetchNode(CollectNode collectNode, MergeNode localMergeNode){
        this.collectNode = collectNode;
        this.localMergeNode = localMergeNode;
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
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
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

    public CollectNode collectNode() {
        return collectNode;
    }

    public MergeNode localMergeNode(){
        return localMergeNode;
    }
}
