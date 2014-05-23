package io.crate.planner.symbol;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import io.crate.operation.Input;
import io.crate.types.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.*;


public class Literal<ReturnType>
        extends DataTypeSymbol
        implements Input<ReturnType>, Comparable<Literal> {

    protected Object value;
    protected DataType type;

    public final static Literal<Void> NULL = new Literal<Void>(DataTypes.NULL, null);
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
        assert value == null ||
                (type.equals(DataTypes.STRING) && value instanceof String) ||
                 // Arrays equality check for array types
                (type.id() == ArrayType.ID && Arrays.equals((Object[])value, (Object[])type.value(value))) ||
                 // types like GeoPoint are represented as arrays
                (value.getClass().isArray() && Arrays.equals((Object[])value, (Object[])type.value(value))) ||
                // converted value must be equal to value otherwise the dataType/value doesn't match
                type.value(value).equals(value);
        this.type = type;
        this.value = value;
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

    public static Literal<Boolean> newLiteral(Boolean value) {
        return value ? BOOLEAN_TRUE : BOOLEAN_FALSE;
    }

    public static Literal<Double> newLiteral(Double value) {
        return new Literal<>(DataTypes.DOUBLE, value);
    }

    public static Literal<Float> newLiteral(Float value) {
        return new Literal<>(DataTypes.FLOAT, value);
    }
}
