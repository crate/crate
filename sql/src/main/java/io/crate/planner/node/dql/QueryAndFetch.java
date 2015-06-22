package io.crate.planner.node.dql;


import io.crate.planner.PlanAndPlannedAnalyzedRelation;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.PlanVisitor;
import io.crate.planner.projection.Projection;

import javax.annotation.Nullable;
import java.util.UUID;

public class QueryAndFetch extends PlanAndPlannedAnalyzedRelation {

    private final CollectNode collectNode;
    private MergeNode localMergeNode;
    private final UUID id;

    public QueryAndFetch(CollectNode collectNode, @Nullable MergeNode localMergeNode, UUID id){
        this.collectNode = collectNode;
        this.localMergeNode = localMergeNode;
        this.id = id;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitQueryAndFetch(this, context);
    }

    @Override
    public UUID jobId() {
        return id;
    }

    public CollectNode collectNode() {
        return collectNode;
    }

    public MergeNode localMergeNode(){
        return localMergeNode;
    }

    @Override
    public void addProjection(Projection projection) {
        DQLPlanNode node = resultNode();
        node.addProjection(projection);
        if (node instanceof CollectNode) {
            PlanNodeBuilder.setOutputTypes((CollectNode) node);
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
