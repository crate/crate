package io.crate.planner.node.dql;


import io.crate.planner.PlanAndPlannedAnalyzedRelation;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.PlanVisitor;
import io.crate.planner.projection.Projection;

import javax.annotation.Nullable;

public class QueryAndFetch extends PlanAndPlannedAnalyzedRelation {

    private final CollectNode collectNode;
    @Nullable
    private MergeNode localMergeNode;

    public QueryAndFetch(CollectNode collectNode, @Nullable MergeNode localMergeNode){
        this.collectNode = collectNode;
        this.localMergeNode = localMergeNode;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitQueryAndFetch(this, context);
    }

    public CollectNode collectNode() {
        return collectNode;
    }

    @Nullable
    public MergeNode localMergeNode(){
        return localMergeNode;
    }

    @Override
    public void addProjection(Projection projection) {
        DQLPlanNode node = resultNode();
        node.addProjection(projection);
        if (node instanceof CollectNode) {
            PlanNodeBuilder.setOutputTypes((CollectNode)node);
        } else if (node instanceof MergeNode) {
            PlanNodeBuilder.connectTypes(collectNode, node);
        }
    }

    @Override
    public boolean resultIsDistributed() {
        return localMergeNode == null;
    }

    @Override
    public DQLPlanNode resultNode() {
        return localMergeNode == null ? collectNode : localMergeNode;
    }
}
