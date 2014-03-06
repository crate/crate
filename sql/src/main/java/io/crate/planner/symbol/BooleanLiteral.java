package io.crate.planner.symbol;

import com.google.common.base.Preconditions;
import org.apache.lucene.util.BytesRef;
import io.crate.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class BooleanLiteral extends Literal<Boolean, BooleanLiteral> {

    private static final StringLiteral TRUE_STRING = new StringLiteral(new BytesRef("t"));
    private static final StringLiteral FALSE_STRING = new StringLiteral(new BytesRef("f"));

    public static final BooleanLiteral TRUE = new BooleanLiteral(true);
    public static final BooleanLiteral FALSE = new BooleanLiteral(false);

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
        return SymbolType.BOOLEAN_LITERAL;
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

    @Override
    public Literal convertTo(DataType type) {
        switch (type) {
            case STRING:
                if (value()) {
                    return TRUE_STRING;
                } else {
                    return FALSE_STRING;
                }
            default:
                return super.convertTo(type);
        }
    }

}
