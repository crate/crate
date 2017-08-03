package io.crate.analyze.symbol;

import com.google.common.base.Preconditions;
import io.crate.data.Input;
import io.crate.exceptions.ConversionException;
import io.crate.types.ArrayType;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.TableType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Literal<ReturnType> extends Symbol implements Input<ReturnType>, Comparable<Literal> {

    private final Object value;
    private final DataType type;

    public final static Literal<Void> NULL = new Literal<>(DataTypes.UNDEFINED, null);
    public final static Literal<Boolean> BOOLEAN_TRUE = new Literal<>(DataTypes.BOOLEAN, true);
    public final static Literal<Boolean> BOOLEAN_FALSE = new Literal<>(DataTypes.BOOLEAN, false);
    public final static Literal<Integer> ZERO = Literal.of(0);
    public static final Literal<Map<String, Object>> EMPTY_OBJECT = Literal.of(Collections.<String, Object>emptyMap());

    public static Collection<Literal> explodeCollection(Literal collectionLiteral) {
        Preconditions.checkArgument(DataTypes.isCollectionType(collectionLiteral.valueType()));
        Iterable values;
        int size;
        Object literalValue = collectionLiteral.value();
        if (literalValue instanceof Collection) {
            values = (Iterable) literalValue;
            size = ((Collection) literalValue).size();
        } else {
            values = Arrays.asList((Object[]) literalValue);
            size = ((Object[]) literalValue).length;
        }

        List<Literal> literals = new ArrayList<>(size);
        for (Object value : values) {
            literals.add(new Literal<>(
                ((CollectionType) collectionLiteral.valueType()).innerType(),
                value
            ));
        }
        return literals;
    }

    public Literal(StreamInput in) throws IOException {
        type = DataTypes.fromStream(in);
        value = type.streamer().readValueFrom(in);
    }

    private Literal(DataType type, ReturnType value) {
        assert typeMatchesValue(type, value) : String.format(Locale.ENGLISH, "value %s is not of type %s", value, type.getName());
        this.type = type;
        this.value = value;
    }

    private static boolean typeMatchesValue(DataType type, Object value) {
        if (value == null) {
            return true;
        }
        if (type.equals(DataTypes.STRING) && (value instanceof BytesRef || value instanceof String)) {
            return true;
        }
        if (type instanceof ArrayType) {
            DataType innerType = ((ArrayType) type).innerType();
            while (innerType instanceof ArrayType && value.getClass().isArray()) {
                type = innerType;
                innerType = ((ArrayType) innerType).innerType();
                value = ((Object[]) value)[0];
            }
            if (innerType.equals(DataTypes.STRING)) {
                for (Object o : ((Object[]) value)) {
                    if (o != null && !(o instanceof String || o instanceof BytesRef)) {
                        return false;
                    }
                }
                return true;
            } else {
                return Arrays.equals((Object[]) value, ((ArrayType) type).value(value));
            }
        }
        // types like GeoPoint are represented as arrays
        if (value.getClass().isArray() && Arrays.equals((Object[]) value, (Object[]) type.value(value))) {
            return true;
        }
        return type.value(value).equals(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(Literal o) {
        return type.compareValueTo(value, o.value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ReturnType value() {
        return (ReturnType) value;
    }

    @Override
    public DataType valueType() {
        return type;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.LITERAL;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitLiteral(this, context);
    }

    @Override
    public int hashCode() {
        if (value == null) {
            return 0;
        }
        if (value.getClass().isArray()) {
            return Arrays.deepHashCode(((Object[]) value));
        }
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Literal literal = (Literal) obj;
        if (valueType().equals(literal.valueType())) {
            if (valueType().compareValueTo(value, literal.value) == 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "Literal{" + stringRepresentation(value) + ", type=" + type + '}';
    }

    private static String stringRepresentation(Object value) {
        if (value == null) {
            return null;
        }
        if (value.getClass().isArray()) {
            return '[' + Stream.of((Object[]) value).map(Literal::stringRepresentation).collect(Collectors.joining(", ")) + ']';
        }
        if (value instanceof BytesRef) {
            return "'" + ((BytesRef) value).utf8ToString() + "'";
        }
        return value.toString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        DataTypes.toStream(type, out);
        type.streamer().writeValueTo(out, value);
    }

    public static Literal<Map<String, Object>> of(Map<String, Object> value) {
        return new Literal<>(DataTypes.OBJECT, value);
    }

    public static Literal<Object[]> of(Object[] value, DataType dataType) {
        return new Literal<>(dataType, value);
    }

    public static Literal<Set> of(Set value, DataType dataType) {
        return new Literal<>(dataType, value);
    }

    public static Literal<Long> of(Long value) {
        return new Literal<>(DataTypes.LONG, value);
    }

    public static Literal<Object> of(DataType type, Object value) {
        return new Literal<>(type, value);
    }

    public static Literal<Integer> of(Integer value) {
        return new Literal<>(DataTypes.INTEGER, value);
    }

    public static Literal<BytesRef> of(String value) {
        if (value == null) {
            return new Literal<>(DataTypes.STRING, null);
        }
        return new Literal<>(DataTypes.STRING, new BytesRef(value));
    }

    public static Literal<BytesRef> of(BytesRef value) {
        return new Literal<>(DataTypes.STRING, value);
    }

    public static Literal<Boolean> of(Boolean value) {
        if (value == null) {
            return new Literal<>(DataTypes.BOOLEAN, null);
        }
        return value ? BOOLEAN_TRUE : BOOLEAN_FALSE;
    }

    public static Literal<Double> of(Double value) {
        return new Literal<>(DataTypes.DOUBLE, value);
    }

    public static Literal<Float> of(Float value) {
        return new Literal<>(DataTypes.FLOAT, value);
    }

    public static Literal<Double[]> newGeoPoint(Object point) {
        return new Literal<>(DataTypes.GEO_POINT, DataTypes.GEO_POINT.value(point));
    }

    public static Literal<Map<String, Object>> newGeoShape(String value) {
        return new Literal<>(DataTypes.GEO_SHAPE, DataTypes.GEO_SHAPE.value(value));
    }

    /**
     * convert the given symbol to a literal with the given type, unless the type already matches,
     * in which case the symbol will be returned as is.
     *
     * @param symbol that is expected to be a literal
     * @param type   type that the literal should have
     * @return converted literal
     * @throws ConversionException if symbol cannot be converted to the given type
     */
    public static Literal convert(Symbol symbol, DataType type) throws ConversionException {
        assert symbol instanceof Literal : "expected a parameter or literal symbol";
        Literal literal = (Literal) symbol;
        if (literal.valueType().equals(type)) {
            return literal;
        }
        try {
            return of(type, type.value(literal.value()));
        } catch (IllegalArgumentException | ClassCastException e) {
            throw new ConversionException(symbol, type);
        }
    }

    @Override
    public String representation() {
        return stringRepresentation(value);
    }
}
