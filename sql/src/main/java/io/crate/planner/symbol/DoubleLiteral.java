package io.crate.planner.symbol;

import com.google.common.base.Preconditions;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class DoubleLiteral extends Literal<Double, DoubleLiteral> {

    public static final SymbolFactory<DoubleLiteral> FACTORY = new SymbolFactory<DoubleLiteral>() {
        @Override
        public DoubleLiteral newInstance() {
            return new DoubleLiteral();
        }
    };
    private double value;

    public DoubleLiteral(Number value) {
        this.value = value.doubleValue();
    }

    DoubleLiteral() {}

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
        value = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(value);
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

        if (Double.compare(that.value, value) != 0) return false;

        return true;
    }

    @Override
    public int hashCode() {
        long temp = Double.doubleToLongBits(value);
        return (int) (temp ^ (temp >>> 32));
    }

    @Override
    public int compareTo(DoubleLiteral o) {
        Preconditions.checkNotNull(o);
        return Integer.signum(Double.compare(value, o.value));
    }
}
