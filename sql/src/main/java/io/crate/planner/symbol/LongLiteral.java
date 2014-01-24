package io.crate.planner.symbol;

import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class LongLiteral extends Literal<Long, LongLiteral> {

    private long value;

    public static final SymbolFactory<LongLiteral> FACTORY = new SymbolFactory<LongLiteral>() {
        @Override
        public LongLiteral newInstance() {
            return new LongLiteral();
        }
    };

    public LongLiteral(long value) {
        this.value = value;
    }

    LongLiteral() {}

    @Override
    public int compareTo(LongLiteral o) {
        return Integer.signum(Long.compare(value, o.value));
    }

    @Override
    public Long value() {
        return value;
    }

    @Override
    public DataType valueType() {
        return DataType.LONG;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.LONG_LITERAL;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitLongLiteral(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        value = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(value);
    }
}
