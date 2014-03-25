package io.crate.planner.symbol;

import com.google.common.base.Preconditions;
import io.crate.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Locale;

public class IntegerLiteral extends NumberLiteral<Integer, IntegerLiteral> {

    private int value;

    public static final SymbolFactory<IntegerLiteral> FACTORY = new SymbolFactory<IntegerLiteral>() {
        @Override
        public IntegerLiteral newInstance() {
            return new IntegerLiteral();
        }
    };

    IntegerLiteral() {}

    public IntegerLiteral(long value) {
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "invalid integer literal %s", value));
        }
        this.value = (int)value;
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
        value = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(value);
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

    @Override
    public int compareTo(IntegerLiteral o) {
        Preconditions.checkNotNull(o);
        return Integer.signum(Integer.compare(value, o.value));
    }
}
