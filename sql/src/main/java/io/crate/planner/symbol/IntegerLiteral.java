package io.crate.planner.symbol;

import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class IntegerLiteral extends Literal<Integer> {

    private int value;

    public static final SymbolFactory<IntegerLiteral> FACTORY = new SymbolFactory<IntegerLiteral>() {
        @Override
        public IntegerLiteral newInstance() {
            return new IntegerLiteral();
        }
    };

    IntegerLiteral() {}

    public IntegerLiteral(int value) {
        this.value = value;
    }

    @Override
    public Integer value() {
        return value;
    }

    @Override
    public DataType valueType() {
        return DataType.INTEGER;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.INTEGER_LITERAL;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitIntegerLiteral(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        value = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IntegerLiteral that = (IntegerLiteral) o;

        if (value != that.value) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return value;
    }
}
