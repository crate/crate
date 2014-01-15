package io.crate.planner.symbol;

public interface Symbol {

    public interface SymbolFactory<T extends Symbol> {
        public T newInstance();
    }

    public SymbolType symbolType();

    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context);
}
