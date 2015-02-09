package io.crate.planner.node.dml;


import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.Path;
import io.crate.planner.Plan;
import io.crate.planner.PlanVisitor;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.symbol.Field;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

public class QueryAndFetch implements PlannedAnalyzedRelation, Plan {

    private final CollectNode collectNode;
    private final MergeNode localMergeNode;

    public QueryAndFetch(CollectNode collectNode, MergeNode localMergeNode){
        this.collectNode = collectNode;
        this.localMergeNode = localMergeNode;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
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
    public Collection<DataType> outputTypes() {
        return localMergeNode.outputTypes();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitQueryAndFetch(this, context);
    }

    public CollectNode collectNode() {
        return collectNode;
    }

    public MergeNode localMergeNode(){
        return localMergeNode;
    }

    @Override
    public Plan plan() {
        return this;
    }
}
