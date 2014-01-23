package io.crate.planner.symbol;

import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class NullLiteral extends Literal<Object> {

    public static final SymbolFactory<NullLiteral> FACTORY = new SymbolFactory<NullLiteral>() {
        @Override
        public NullLiteral newInstance() {
            return new NullLiteral();
        }
    };

    private DataType type;

    public NullLiteral(DataType type) {
        this.type = type;
    }

    NullLiteral() {}

    @Override
    public Object value() {
        return null;
    }

    @Override
    public DataType valueType() {
        return type;
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
    public void readFrom(StreamInput in) throws IOException {
        type = DataType.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        DataType.toStream(type, out);
    }
}
