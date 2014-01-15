package io.crate.planner.symbol;

import io.crate.metadata.FunctionIdent;

import java.util.List;

public class Aggregation implements Symbol {

    public static final SymbolFactory<Aggregation> FACTORY = new SymbolFactory<Aggregation>() {
        @Override
        public Aggregation newInstance() {
            return new Aggregation();
        }
    };

    public Aggregation() {

    }

    public static enum Step {
        ITER, PARTIAL, FINAL
    }

    private FunctionIdent functionIdent;
    private List<ValueSymbol> inputs;
    private Step fromStep;
    private Step toStep;

    public Aggregation(FunctionIdent functionIdent, List<ValueSymbol> inputs, Step fromStep, Step toStep) {
        this.functionIdent = functionIdent;
        this.inputs = inputs;
        this.fromStep = fromStep;
        this.toStep = toStep;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.AGGREGATION;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitAggregation(this, context);
    }

    public FunctionIdent functionIdent() {
        return functionIdent;
    }

    public List<ValueSymbol> inputs() {
        return inputs;
    }

    public Step fromStep() {
        return fromStep;
    }

    public Step toStep() {
        return toStep;
    }
}
