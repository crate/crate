package io.crate.planner.symbol;

import com.google.common.base.Preconditions;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class BooleanLiteral extends Literal<Boolean, BooleanLiteral> {

    private Boolean value;

    public static final SymbolFactory<BooleanLiteral> FACTORY = new SymbolFactory<BooleanLiteral>() {
        @Override
        public BooleanLiteral newInstance() {
            return new BooleanLiteral();
        }
    };

    public BooleanLiteral(Boolean value) {
        this.value = value;
    }

    BooleanLiteral() {
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.BOOlEAN_LITERAL;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitBooleanLiteral(this, context);
    }

    @Override
    public Boolean value() {
        return value;
    }

    @Override
    public DataType valueType() {
        return DataType.BOOLEAN;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        value = in.readOptionalBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBoolean(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BooleanLiteral that = (BooleanLiteral) o;

        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }

    @Override
    public int compareTo(BooleanLiteral o) {
        Preconditions.checkNotNull(o);
        return Integer.signum(Boolean.compare(value, o.value));
    }
}
