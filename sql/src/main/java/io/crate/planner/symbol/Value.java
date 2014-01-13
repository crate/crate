package io.crate.planner.symbol;

import org.cratedb.DataType;

public class Value implements ValueSymbol {

    public static final SymbolType TYPE = SymbolType.VALUE;

    public static final SymbolFactory<Value> FACTORY = new SymbolFactory<Value>() {
        @Override
        public Value newInstance() {
            return new Value();
        }
    };

    private DataType type;

    public Value(DataType type) {
        this.type = type;
    }

    public Value() {

    }

    public DataType valueType() {
        return type;
    }

    @Override
    public SymbolType symbolType() {
        return TYPE;
    }


}
