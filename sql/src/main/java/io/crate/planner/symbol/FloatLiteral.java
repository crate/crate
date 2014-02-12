package io.crate.planner.symbol;

import com.google.common.base.Preconditions;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class FloatLiteral extends NumberLiteral<Float, FloatLiteral> {

    private float value;

    public static final SymbolFactory<FloatLiteral> FACTORY = new SymbolFactory<FloatLiteral>() {
        @Override
        public FloatLiteral newInstance() {
            return new FloatLiteral();
        }
    };

    FloatLiteral() {}

    public FloatLiteral(Number value) {
        Preconditions.checkNotNull(value);
        this.value = value.floatValue();
    }

    @Override
    public int compareTo(FloatLiteral o) {
        return Integer.signum(Float.compare(value, o.value));
    }

    @Override
    public Float value() {
        return value;
    }

    @Override
    public DataType valueType() {
        return DataType.FLOAT;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.FLOAT_LITERAL;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitFloatLiteral(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        value = in.readFloat();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeFloat(value);
    }
}
