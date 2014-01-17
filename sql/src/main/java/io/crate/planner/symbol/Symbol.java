package io.crate.planner.symbol;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public abstract class Symbol implements Streamable {

    public interface SymbolFactory<T extends Symbol> {
        public T newInstance();
    }

    public abstract SymbolType symbolType();

    public abstract <C, R> R accept(SymbolVisitor<C, R> visitor, C context);

    public static void toStream(Symbol symbol, StreamOutput out) throws IOException {
        out.writeVInt(symbol.symbolType().ordinal());
        symbol.writeTo(out);
    }

    public static Symbol fromStream(StreamInput in) throws IOException {
        Symbol symbol = SymbolType.values()[in.readVInt()].newInstance();
        symbol.readFrom(in);

        return symbol;
    }
}
