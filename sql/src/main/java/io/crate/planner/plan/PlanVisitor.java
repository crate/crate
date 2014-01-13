package io.crate.planner.plan;

// PRESTOBORROW


public class PlanVisitor<C, R> {

    protected void visitSources(PlanNode node, C context){
        if (node.sources() != null){
            for (PlanNode source: node.sources()){
                source.accept(this, context);
            }
        }
    }

    protected R visitPlan(PlanNode node, C context) {
        return null;
    }

    public R visitAggregation(AggregationNode node, C context) {
        return visitPlan(node, context);
    }

    public R visitCollect(CollectNode node, C context) {
        return visitPlan(node, context);
    }
}
