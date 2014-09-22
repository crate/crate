package io.crate.planner.symbol;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.spatial4j.core.shape.Shape;
import io.crate.operation.Input;
import io.crate.types.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;

import java.io.IOException;
import java.util.*;


public class Literal<ReturnType>
        extends DataTypeSymbol
        implements Input<ReturnType>, Comparable<Literal> {

    protected Object value;
    protected DataType type;

    public final static Literal<Void> NULL = new Literal<Void>(DataTypes.NULL, null);
    public final static Literal<BytesRef> EMPTY_STRING = new Literal<BytesRef>(DataTypes.STRING, new BytesRef());
    private final static Literal<Boolean> BOOLEAN_TRUE = new Literal<>(DataTypes.BOOLEAN, true);
    private final static Literal<Boolean> BOOLEAN_FALSE = new Literal<>(DataTypes.BOOLEAN, false);

    public static final SymbolFactory<Literal> FACTORY = new SymbolFactory<Literal>() {
        @Override
        public Literal newInstance() {
            return new Literal();
        }
    };

    /**
     * guesses the type from the parameters value and creates a new Literal
     */
    public static Literal<?> fromParameter(Parameter parameter) {
        DataType<?> dataType = DataTypes.guessType(parameter.value(), true);
        if (dataType.equals(DataTypes.STRING)) {
            // force conversion to bytesRef
            return new Literal<>(dataType, dataType.value(parameter.value()));
        }
        // all other types are okay as is.
        return new Literal<>(dataType, parameter.value());
    }

    public static Literal implodeCollection(DataType itemType, Set<Literal> literals) {
        ImmutableSet.Builder<Object> builder = ImmutableSet.builder();
        for (Literal literal : literals) {
            assert literal.valueType() == itemType :
                    String.format("Literal type: %s does not match item type: %s", literal.valueType(), itemType);
            builder.add(literal.value());
        }
        return new Literal<>(new SetType(itemType), builder.build());
    }

    public static Literal implodeCollection(DataType itemType, List<Literal> literals) {
       Object[] values = new Object[literals.size()];
        for (int i = 0; i<literals.size(); i++) {
            assert literals.get(i).valueType() == itemType :
                    String.format("Literal type: %s does not match item type: %s",
                            literals.get(i).valueType(), itemType);
            values[i] = literals.get(i).value();
        }
        return new Literal<>(new ArrayType(itemType), values);
    }

    public static Collection<Literal> explodeCollection(Literal collectionLiteral) {
        Preconditions.checkArgument(DataTypes.isCollectionType(collectionLiteral.valueType()));
        Collection values = (Collection) collectionLiteral.value();

        List<Literal> literals = new ArrayList<>(values.size());
        for (Object value : values) {
            literals.add(new Literal<>(
                    ((CollectionType)collectionLiteral.valueType()).innerType(),
                    value
            ));
        }
        return literals;
    }

    protected Literal() {
    }

    protected Literal(DataType type, ReturnType value) {
        assert typeMatchesValue(type, value) : String.format("value %s is not of type %s", value, type.getName());
        this.type = type;
        this.value = value;
    }

    private static <ReturnType> boolean typeMatchesValue(DataType type, ReturnType value) {
        if (value == null) {
            return true;
        }
        if (type.equals(DataTypes.STRING) && (value instanceof BytesRef || value instanceof String)) {
            return true;
        }
        if (type instanceof ArrayType) {
            DataType innerType = ((ArrayType) type).innerType();
            if (innerType.equals(DataTypes.STRING)) {
                for (Object o : ((Object[]) value)) {
                    if (o != null && !(o instanceof String || o instanceof BytesRef)) {
                        return false;
                    }
                }
                return true;
            } else {
                return Arrays.equals(((Object[]) value), (Object[]) type.value(value));
            }
        }
        // types like GeoPoint are represented as arrays
        if (value.getClass().isArray() && Arrays.equals(((Object[]) value), ((Object[]) type.value(value)))) {
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
        return (ReturnType)value;
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
        return Objects.hashCode(value());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Literal literal = (Literal) obj;
        if (valueType().equals(literal.valueType())) {
            return Objects.equals(value(), literal.value());
        }
        return false;
    }

    @Override
    public String toString() {
        return "Literal{" +
                "value=" + BytesRefs.toString(value) +
                ", type=" + type +
                '}';
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readFrom(StreamInput in) throws IOException {
        type = DataTypes.fromStream(in);
        value = type.streamer().readValueFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        DataTypes.toStream(type, out);
        type.streamer().writeValueTo(out, value);
    }

    public static Literal<Map<String, Object>> newLiteral(Map<String, Object> value) {
        return new Literal<>(DataTypes.OBJECT, value);
    }

    public static Literal<Object[]> newLiteral(Object[] value, DataType dataType) {
        return new Literal<>(dataType, value);
    }

    public static Literal<Long> newLiteral(Long value) {
        return new Literal<>(DataTypes.LONG, value);
    }

    public static Literal<Object> newLiteral(DataType type, Object value) {
        return new Literal<>(type, value);
    }

    public static Literal<Integer> newLiteral(Integer value) {
        return new Literal<>(DataTypes.INTEGER, value);
    }

    public static Literal<BytesRef> newLiteral(String value) {
        if (value == null) {
            return new Literal<>(DataTypes.STRING, null);
        }
        return new Literal<>(DataTypes.STRING, new BytesRef(value));
    }

    public static Literal<BytesRef> newLiteral(BytesRef value) {
        return new Literal<>(DataTypes.STRING, value);
    }

    public static Literal<Boolean> newLiteral(Boolean value) {
        if (value == null) {
            return new Literal<>(DataTypes.BOOLEAN, null);
        }
        return value ? BOOLEAN_TRUE : BOOLEAN_FALSE;
    }

    public static Literal<Double> newLiteral(Double value) {
        return new Literal<>(DataTypes.DOUBLE, value);
    }

    public static Literal<Float> newLiteral(Float value) {
        return new Literal<>(DataTypes.FLOAT, value);
    }

    public static Literal<Shape> newGeoShape(String value) {
        return new Literal<>(DataTypes.GEO_SHAPE, DataTypes.GEO_SHAPE.value(value));
    }

    public static Literal toLiteral(Symbol symbol, DataType type) throws IllegalArgumentException {
        switch (symbol.symbolType()) {
            case PARAMETER:
                return Literal.newLiteral(type, type.value(((Parameter) symbol).value()));
            case LITERAL:
                Literal literal = (Literal)symbol;
                if (literal.valueType().equals(type)) {
                    return literal;
                }
                return Literal.newLiteral(type, type.value(literal.value()));
        }
        throw new IllegalArgumentException("expected a parameter or literal symbol");
    }

    public static Literal toLiteral(Symbol s) {
        switch (s.symbolType()) {
            case PARAMETER:
                return Literal.fromParameter((Parameter) s);
            case LITERAL:
                return (Literal) s;
            default:
                throw new IllegalArgumentException("expected a parameter or literal symbol");
        }
    }
}
