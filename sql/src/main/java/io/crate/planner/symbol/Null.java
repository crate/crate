package io.crate.planner.symbol;

import io.crate.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class Null extends Literal<Void, Null> {

    public static final Null INSTANCE = new Null();
    public static final SymbolFactory<Null> FACTORY = new SymbolFactory<Null>() {
        @Override
        public Null newInstance() {
            return INSTANCE;
        }
    };

    private Null() {
    }

    @Override
    public DataType valueType() {
        return DataType.NULL;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.NULL_LITERAL;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitNullLiteral(this, context);
    }

    @Override
    public Void value() {
        return null;
    }

    /**
     * null literals can occur anywhere
     * @param type
     * @return
     */
    @Override
    public Literal convertTo(DataType type) {
        return INSTANCE;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    @Override
    public int compareTo(Null o) {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
