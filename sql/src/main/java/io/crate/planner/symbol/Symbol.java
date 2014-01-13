package io.crate.planner.symbol;

import com.google.common.base.Predicate;

public interface Symbol {

    public interface SymbolFactory<T extends Symbol>{
        public T newInstance();
    }

    public SymbolType symbolType();

}
