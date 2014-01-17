package io.crate.planner.symbol;

public enum SymbolType {


    AGGREGATION(Aggregation.FACTORY),
    REFERENCE(Reference.FACTORY),
    VALUE(Value.FACTORY),
    ROUTING(Routing.FACTORY);

    private final Symbol.SymbolFactory factory;

    SymbolType(Symbol.SymbolFactory factory) {
        this.factory = factory;
    }

    public Symbol newInstance() {
        return factory.newInstance();
    }

}
