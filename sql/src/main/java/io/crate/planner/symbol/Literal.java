package io.crate.planner.symbol;

import io.crate.operation.Input;
import org.apache.lucene.util.BytesRef;
import io.crate.DataType;

import java.util.List;
import java.util.Map;
import java.util.Objects;


@SuppressWarnings("unchecked")
public abstract class Literal<ValueType, LiteralType> extends ValueSymbol
        implements Input<ValueType>, Comparable<LiteralType> {

    public static Literal forType(DataType type, Object value) {
        /**
         * NOTE: {@link io.crate.DataType} should be changed into a class
         * and then creating an literal for a type can be done using polymorphism.
         */

        if (value == null) {
            return Null.INSTANCE;
        }

        switch (type) {
            case BYTE:
                return new ByteLiteral(((Number) value).longValue());
            case SHORT:
                return new ShortLiteral(((Number) value).longValue());
            case INTEGER:
                return new IntegerLiteral(((Number) value).longValue());
            case TIMESTAMP:
                if (value instanceof BytesRef) {
                    return new TimestampLiteral((BytesRef) value);
                } else if (value instanceof String) {
                    return new TimestampLiteral((String)value);
                } else {
                    return new TimestampLiteral((Long)value);
                }
            case LONG:
                return new LongLiteral((Long) value);
            case FLOAT:
                return new FloatLiteral(((Number) value).floatValue());
            case DOUBLE:
                return new DoubleLiteral(((Number) value).doubleValue());
            case BOOLEAN:
                return new BooleanLiteral((Boolean) value);
            case IP:
            case STRING:
                if (value instanceof BytesRef) {
                    return new StringLiteral((BytesRef) value);
                } else {
                    return new StringLiteral(value.toString());
                }
            case OBJECT:
                return new ObjectLiteral((Map<String, Object>) value);
            case NOT_SUPPORTED:
                throw new UnsupportedOperationException();
        }

        if (DataType.ARRAY_TYPES.contains(type)) {
            return createArrayLiteral(type, value);
        }

        return null;
    }

    private static Literal createArrayLiteral(DataType type, Object value) {
        DataType itemType = DataType.REVERSE_ARRAY_TYPE_MAP.get(type);
        if (value.getClass().isArray()) {
            return new ArrayLiteral(itemType, (Object[])value);
        }
        return new ArrayLiteral(itemType, ((List)value).toArray(new Object[((List)value).size()]));
    }

    public String valueAsString() {
        return value().toString();
    }

    public static Literal forValue(Object value) {
        return forValue(value, true);
    }

    /**
     * create a literal for a given Java object
     * @param value the value to wrap/transform into a literal
     * @return a literal of a guessed type, holding the value object
     * @throws java.lang.IllegalArgumentException if value cannot be wrapped into a <code>Literal</code>
     */
    public static Literal forValue(Object value, boolean strict) {
        DataType type = DataType.forValue(value, strict);
        if (type == null) {
            throw new IllegalArgumentException(
                    String.format("value of unsupported class '%s'", value.getClass().getSimpleName()));
        }
        return forType(type, value);
    }

    public Object convertValueTo(DataType type, ValueType value) {
        if (valueType() == type) {
            return value;
        } else if (type == DataType.NOT_SUPPORTED) {
            return Null.INSTANCE;
        }

        throw new UnsupportedOperationException(
                "Invalid input for type " + type.getName() + ": " + value().toString());
    }

    public final Object convertValueTo(DataType type) {
        return convertValueTo(type, value());
    }

    public Literal convertTo(DataType type) {
        if (valueType() == type) {
            return this;
        } else if (type == DataType.NOT_SUPPORTED) {
            return Null.INSTANCE;
        }
        throw new UnsupportedOperationException(
                "Invalid input for type " + type.getName() + ": " + value().toString());
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
