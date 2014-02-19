package io.crate.planner.symbol;

import io.crate.operator.Input;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;

import java.util.Map;
import java.util.Objects;


@SuppressWarnings("unchecked")
public abstract class Literal<ValueType, LiteralType> extends ValueSymbol
        implements Input<ValueType>, Comparable<LiteralType> {

    public static Literal forType(DataType type, Object value) {
        if (value == null) {
            return Null.INSTANCE;
        }

        switch (type) {
            case BYTE:
                return new IntegerLiteral(((Byte) value).intValue());
            case SHORT:
                return new IntegerLiteral(((Short) value).intValue());
            case INTEGER:
                return new IntegerLiteral((Integer) value);
            case TIMESTAMP:
            case LONG:
                return new LongLiteral((Long) value);
            case FLOAT:
                return new FloatLiteral((Float) value);
            case DOUBLE:
                return new DoubleLiteral((Double) value);
            case BOOLEAN:
                return new BooleanLiteral((Boolean) value);
            case IP:
            case STRING:
                if (value instanceof BytesRef) {
                    return new StringLiteral((BytesRef) value);
                } else {
                    return new StringLiteral((String) value);
                }
            case OBJECT:
                return new ObjectLiteral((Map<String, Object>) value);
            case NOT_SUPPORTED:
                throw new UnsupportedOperationException();
        }

        return null;
    }

    public String valueAsString() {
        return value().toString();
    }

    /**
     * create a literal for a given Java object
     * @param value the value to wrap/transform into a literal
     * @return a literal of a guessed type, holding the value object
     * @throws java.lang.IllegalArgumentException if value cannot be wrapped into a <code>Literal</code>
     */
    public static Literal forValue(Object value) {
        DataType type = DataType.forValue(value);
        if (type == null) {
            throw new IllegalArgumentException(
                    String.format("value of unsupported class '%s'", value.getClass().getSimpleName()));
        }
        return forType(type, value);
    }

    public Literal convertTo(DataType type) {
        if (valueType() == type) {
            return this;
        }
        throw new UnsupportedOperationException("Invalid input for type " + type.getName() + ": " + value().toString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Literal literal = (Literal) o;
        if (valueType() == literal.valueType()) {
            return Objects.equals(value(), literal.value());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value());
    }

}
