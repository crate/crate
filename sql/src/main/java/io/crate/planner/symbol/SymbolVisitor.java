package io.crate.planner.symbol;

// PRESTOBORROW


import io.crate.planner.plan.PlanNode;
import org.elasticsearch.common.Nullable;

public class SymbolVisitor<C, R> {

    public void processSymbols(PlanNode node, C context) {
        if (node.symbols() != null) {
            for (Symbol symbol : node.symbols()) {
                symbol.accept(this, context);
            }
        }
    }

    public R process(Symbol symbol, @Nullable C context) {
        return symbol.accept(this, context);
    }


    protected R visitSymbol(Symbol symbol, C context) {
        return null;
    }

    public R visitAggregation(Aggregation symbol, C context) {
        return visitSymbol(symbol, context);
    }

    public R visitValue(Value symbol, C context) {
        return visitSymbol(symbol, context);
    }

    public R visitReference(Reference symbol, C context) {
        return visitSymbol(symbol, context);
    }

}
