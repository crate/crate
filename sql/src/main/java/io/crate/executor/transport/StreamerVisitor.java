package io.crate.executor.transport;

import io.crate.planner.symbol.*;
import org.cratedb.DataType;

public class StreamerVisitor extends SymbolVisitor<Void, DataType.Streamer> {

    @Override
    public DataType.Streamer visitValue(Value symbol, Void context) {
        return symbol.valueType().streamer();
    }

    @Override
    public DataType.Streamer visitReference(Reference symbol, Void context) {
        return symbol.valueType().streamer();
    }

    @Override
    protected DataType.Streamer visitSymbol(Symbol symbol, Void context) {
        throw new UnsupportedOperationException("Can't get a streamer for symbol " + symbol);
    }
}
