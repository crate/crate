package io.crate.planner;

import io.crate.analyze.Analysis;
import io.crate.planner.plan.AggregationNode;
import io.crate.planner.plan.CollectNode;
import io.crate.planner.plan.TopNNode;
import io.crate.planner.symbol.*;
import io.crate.sql.tree.DefaultTraversalVisitor;
import org.cratedb.DataType;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.inject.Singleton;

import java.util.Collection;
import java.util.List;

@Singleton
public class Planner extends DefaultTraversalVisitor<Symbol, Analysis> {

    public Plan plan(Analysis analysis) {
        analysis.query();
        //RelationPlanner planner = new RelationPlanner(analysis, symbolAllocator, idAllocator, metadata, session);
        //plan = planner.process(analysis.getQuery(), null);

        // with node row granularity we are sure we have only one result per collect
        Preconditions.checkArgument(analysis.rowGranularity() == RowGranularity.NODE,
                "unsupported row granularity", analysis.rowGranularity());

        Reference[] references = (Reference[]) analysis.references().toArray();

        CollectNode collectNode = new CollectNode();
        collectNode.routing(analysis.routing());
        collectNode.outputs(references);

        if (analysis.hasAggregates()) {
            if (analysis.hasGroupBy()) {
                throw new UnsupportedOperationException("query not supported");
            } else {
                AggregationNode aggregationNode = new AggregationNode("aggregate");
                aggregationNode.source(collectNode);
////
////                Aggregation[] aggs = new Aggregation[analysis.aggregations().size()];
////                int i = 0;
////                for (Aggregation agg: analysis.aggregations()){
////                    aggs[i++] = agg.split(Aggregation.Step.ITER, Aggregation.Step.FINAL);
////                }
////
////                aggregationNode.inputs(getInputs(cn.outputs()));
////
////                ValueSymbol value = new Value(DataType.DOUBLE);
////                FunctionIdent fi = new FunctionIdent("avg", ImmutableList.of(value.valueType()));
////                Aggregation agg = new Aggregation(fi, ImmutableList.of(value), Aggregation.Step.ITER, Aggregation.Step.FINAL);
////
////
////
//                aggregationNode.symbols(value, agg);
//                aggregationNode.inputs(value);
//                aggregationNode.outputs(agg);


                // TODO:
            }
        } else {
            if (analysis.hasGroupBy()) {
                throw new UnsupportedOperationException("query not supported");
            } else {
                TopNNode topNNode;
                Value[] inputs = getInputs(collectNode.outputs());
                if (analysis.isSorted()) {
                    topNNode = sortedTopN(collectNode.outputs(), analysis);
                } else {
                    topNNode = new TopNNode("topn", analysis.limit(), 0);
                }
                topNNode.source(collectNode);
                topNNode.outputs(inputs);
            }
        }
        return null;

    }

    private Value[] getInputs(List<Symbol> outputs) {
        Value[] inputs = new Value[outputs.size()];
        for (int i = 0; i < inputs.length; i++) {
            inputs[i] = new Value(((ValueSymbol) outputs.get(i)).valueType());
        }
        return inputs;
    }

    private TopNNode sortedTopN(List<Symbol> outputs, Analysis analysis) {
        int[] orderBy = new int[analysis.sortSymbols().size()];
        int idx = 0;
        int orderIdx = 0;
        for (Symbol s : outputs) {
            if (analysis.sortSymbols().contains(s)) {
                orderBy[orderIdx++] = idx;
            }
            idx++;
        }
        TopNNode node = new TopNNode("sortedtopn", analysis.limit(), 0, orderBy, analysis.reverseFlags());
        return node;
    }
}
