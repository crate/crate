package io.crate.planner.node.dql;


import io.crate.planner.PlanAndPlannedAnalyzedRelation;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.PlanVisitor;
import io.crate.planner.projection.Projection;

import javax.annotation.Nullable;
import java.util.UUID;

public class QueryAndFetch extends PlanAndPlannedAnalyzedRelation {

    private final CollectPhase collectNode;
    private MergePhase localMergeNode;
    private final UUID id;

    public QueryAndFetch(CollectPhase collectNode, @Nullable MergePhase localMergeNode, UUID id){
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

    public CollectPhase collectNode() {
        return collectNode;
    }

    public MergePhase localMergeNode(){
        return localMergeNode;
    }

    @Override
    public void addProjection(Projection projection) {
        DQLPlanNode node = resultNode();
        node.addProjection(projection);
        if (node instanceof CollectPhase) {
            PlanNodeBuilder.setOutputTypes((CollectPhase) node);
        } else if (node instanceof MergePhase) {
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
