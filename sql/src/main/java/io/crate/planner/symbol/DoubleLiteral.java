package io.crate.planner.symbol;

import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class DoubleLiteral extends Literal<Double> {

    public static final SymbolFactory<DoubleLiteral> FACTORY = new SymbolFactory<DoubleLiteral>() {
        @Override
        public DoubleLiteral newInstance() {
            return new DoubleLiteral();
        }
    };
    private Double value;

    public DoubleLiteral(Number value) {
        this.value = value.doubleValue();
    }

    public DoubleLiteral() {

    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.DOUBLE_LITERAL;
    }

    @Override
    public Double value() {
        return value;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitDoubleLiteral(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            value = in.readDouble();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        boolean hasValue = value != null;
        out.writeBoolean(hasValue);
        if (hasValue) {
            out.writeDouble(value);
        }
    }

    @Override
    public DataType valueType() {
        return DataType.DOUBLE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DoubleLiteral that = (DoubleLiteral) o;

        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }
}
