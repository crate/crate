/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.types;

import static java.util.Map.entry;
import static java.util.stream.Collectors.toSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.RandomAccess;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.jetbrains.annotations.Nullable;
import org.locationtech.spatial4j.shape.impl.PointImpl;
import org.locationtech.spatial4j.shape.jts.JtsPoint;

import io.crate.Streamer;
import io.crate.sql.tree.BitString;

public final class DataTypes {

    // DataTypes is initialized early before the Log configuration has been done;
    // Need to defer the Logger initialized to first use.
    static class Lazy {
        private static final Logger LOGGER = LogManager.getLogger(DataTypes.class);
    }

    /**
     * If you add types here make sure to update the SizeEstimatorFactory in the SQL module.
     */
    public static final UndefinedType UNDEFINED = UndefinedType.INSTANCE;
    public static final NotSupportedType NOT_SUPPORTED = NotSupportedType.INSTANCE;

    public static final ByteType BYTE = ByteType.INSTANCE;
    public static final BooleanType BOOLEAN = BooleanType.INSTANCE;

    public static final StringType STRING = StringType.INSTANCE;
    public static final CharacterType CHARACTER = CharacterType.INSTANCE;
    public static final IpType IP = IpType.INSTANCE;

    public static final DoubleType DOUBLE = DoubleType.INSTANCE;
    public static final FloatType FLOAT = FloatType.INSTANCE;

    public static final ShortType SHORT = ShortType.INSTANCE;
    public static final IntegerType INTEGER = IntegerType.INSTANCE;
    public static final LongType LONG = LongType.INSTANCE;
    public static final NumericType NUMERIC = NumericType.INSTANCE;

    public static final TimeTZType TIMETZ = TimeTZType.INSTANCE;

    public static final TimestampType TIMESTAMPZ = TimestampType.INSTANCE_WITH_TZ;
    public static final TimestampType TIMESTAMP = TimestampType.INSTANCE_WITHOUT_TZ;

    public static final DateType DATE = DateType.INSTANCE;

    public static final GeoPointType GEO_POINT = GeoPointType.INSTANCE;
    public static final GeoShapeType GEO_SHAPE = GeoShapeType.INSTANCE;

    public static final ArrayType<Double> DOUBLE_ARRAY = new ArrayType<>(DOUBLE);
    public static final ArrayType<Float> FLOAT_ARRAY = new ArrayType<>(FLOAT);
    public static final ArrayType<String> STRING_ARRAY = new ArrayType<>(STRING);
    public static final ArrayType<Integer> INTEGER_ARRAY = new ArrayType<>(INTEGER);
    public static final ArrayType<Short> SHORT_ARRAY = new ArrayType<>(SHORT);
    public static final ArrayType<Long> BIGINT_ARRAY = new ArrayType<>(LONG);
    public static final ArrayType<Boolean> BOOLEAN_ARRAY = new ArrayType<>(BOOLEAN);

    public static final List<ArrayType<?>> ARRAY_TYPES = List.of(
        DOUBLE_ARRAY,
        FLOAT_ARRAY,
        STRING_ARRAY,
        INTEGER_ARRAY,
        SHORT_ARRAY,
        BIGINT_ARRAY,
        BOOLEAN_ARRAY
    );

    @Nullable
    public static ArrayType<?> getArrayTypeByInnerType(DataType<?> innerType) {
        for (ArrayType<?> arrayType : ARRAY_TYPES) {
            if (arrayType.innerType().equals(innerType)) {
                return arrayType;
            }
        }
        return null;
    }

    public static final OidVectorType OIDVECTOR = new OidVectorType();

    public static final IntervalType INTERVAL = IntervalType.INSTANCE;

    public static final ObjectType UNTYPED_OBJECT = ObjectType.UNTYPED;

    public static final RegprocType REGPROC = RegprocType.INSTANCE;
    public static final RegclassType REGCLASS = RegclassType.INSTANCE;

    public static final List<DataType<?>> PRIMITIVE_TYPES = List.of(
        BYTE,
        BOOLEAN,
        CHARACTER,
        STRING,
        IP,
        DOUBLE,
        FLOAT,
        SHORT,
        INTEGER,
        INTERVAL,
        LONG,
        TIMESTAMPZ,
        TIMESTAMP,
        DATE
    );

    private static final Set<Integer> PRIMITIVE_TYPE_IDS =
        PRIMITIVE_TYPES.stream()
            .map(DataType::id)
            .collect(toSet());


    public static final List<DataType<? extends Number>> NUMERIC_PRIMITIVE_TYPES = List.of(
        DOUBLE,
        FLOAT,
        BYTE,
        SHORT,
        INTEGER,
        LONG
    );

    private static final Set<Integer> NUMERIC_PRIMITIVE_TYPE_IDS =
        NUMERIC_PRIMITIVE_TYPES.stream()
            .map(DataType::id)
            .collect(toSet());

    /**
     * Type registry mapping type ids to the according data type instance.
     */
    private static final Map<Integer, Writeable.Reader<DataType<?>>> TYPE_REGISTRY = new HashMap<>(
        Map.ofEntries(
            entry(UndefinedType.ID, in -> UNDEFINED),
            entry(NotSupportedType.ID, in -> NOT_SUPPORTED),
            entry(ByteType.ID, in -> BYTE),
            entry(BooleanType.ID, in -> BOOLEAN),
            entry(CharacterType.ID, CharacterType::new),
            entry(StringType.ID, StringType::new),
            entry(IpType.ID, in -> IP),
            entry(DoubleType.ID, in -> DOUBLE),
            entry(FloatType.ID, in -> FLOAT),
            entry(ShortType.ID, in -> SHORT),
            entry(IntegerType.ID, in -> INTEGER),
            entry(LongType.ID, in -> LONG),
            entry(NumericType.ID, NumericType::new),
            entry(TimeTZType.ID, in -> TIMETZ),
            entry(TimestampType.ID_WITH_TZ, in -> TIMESTAMPZ),
            entry(TimestampType.ID_WITHOUT_TZ, in -> TIMESTAMP),
            entry(ObjectType.ID, ObjectType::new),
            entry(UncheckedObjectType.ID, in -> UncheckedObjectType.INSTANCE),
            entry(GeoPointType.ID, in -> GEO_POINT),
            entry(GeoShapeType.ID, in -> GEO_SHAPE),
            entry(ArrayType.ID, ArrayType::new),
            entry(IntervalType.ID, in -> INTERVAL),
            entry(RowType.ID, RowType::new),
            entry(RegprocType.ID, in -> REGPROC),
            entry(RegclassType.ID, in -> REGCLASS),
            entry(OidVectorType.ID, in -> OIDVECTOR),
            entry(DateType.ID, in -> DATE),
            entry(BitStringType.ID, BitStringType::new),
            entry(JsonType.ID, in -> JsonType.INSTANCE),
            entry(FloatVectorType.ID, FloatVectorType::new)
        )
    );

    private static final Set<Integer> NUMBER_CONVERSIONS = Stream.concat(
        Stream.of(BOOLEAN, STRING, TIMESTAMPZ, TIMESTAMP, DATE, IP, NUMERIC, CHARACTER),
        NUMERIC_PRIMITIVE_TYPES.stream()
    ).map(DataType::id).collect(toSet());

    // allowed conversion from key to one of the value types
    // the key type itself does not need to be in the value set
    static final Map<Integer, Set<Integer>> ALLOWED_CONVERSIONS = Map.ofEntries(
        entry(BYTE.id(), NUMBER_CONVERSIONS),
        entry(SHORT.id(), NUMBER_CONVERSIONS),
        entry(INTEGER.id(), Stream.concat(
            NUMBER_CONVERSIONS.stream(),
            Stream.of(RegprocType.ID, RegclassType.ID)
        ).collect(Collectors.toUnmodifiableSet())),
        entry(REGPROC.id(), Set.of(STRING.id(), INTEGER.id(), CHARACTER.id())),
        entry(REGCLASS.id(), Set.of(STRING.id(), INTEGER.id(), LONG.id(), CHARACTER.id())),
        entry(
            LONG.id(),
            Stream.concat(
                NUMBER_CONVERSIONS.stream(),
                Stream.of(RegprocType.ID, RegclassType.ID)
            ).collect(Collectors.toUnmodifiableSet())
        ),
        entry(NUMERIC.id(), NUMBER_CONVERSIONS),
        entry(FLOAT.id(), NUMBER_CONVERSIONS),
        entry(DOUBLE.id(), NUMBER_CONVERSIONS),
        entry(BOOLEAN.id(), Set.of(STRING.id(), CHARACTER.id())),
        entry(STRING.id(), Stream.concat(
            Stream.of(
                GEO_SHAPE.id(),
                GEO_POINT.id(),
                ObjectType.ID,
                RegprocType.ID,
                RegclassType.ID,
                TimeTZType.ID,
                BitStringType.ID,
                JsonType.ID,
                CharacterType.ID
            ),
            NUMBER_CONVERSIONS.stream()
        ).collect(toSet())),
        entry(CHARACTER.id(), Stream.concat(
            Stream.of(
                GEO_SHAPE.id(),
                GEO_POINT.id(),
                ObjectType.ID,
                RegprocType.ID,
                RegclassType.ID,
                TimeTZType.ID,
                BitStringType.ID,
                JsonType.ID,
                StringType.ID
            ),
            NUMBER_CONVERSIONS.stream()
        ).collect(toSet())),
        entry(IP.id(), Set.of(STRING.id(), CHARACTER.id())),
        entry(TIMESTAMPZ.id(), Set.of(DOUBLE.id(), LONG.id(), STRING.id(), TIMESTAMP.id(), CHARACTER.id(), DATE.id())),
        entry(TIMESTAMP.id(), Set.of(DOUBLE.id(), LONG.id(), STRING.id(), TIMESTAMPZ.id(), CHARACTER.id(), DATE.id())),
        entry(DATE.id(), Set.of(DOUBLE.id(), LONG.id(), STRING.id(), TIMESTAMP.id(), TIMESTAMPZ.id(), CHARACTER.id())),
        entry(UNDEFINED.id(), Set.of()), // actually convertible to every type, see NullType
        entry(GEO_POINT.id(), Set.of()),
        entry(GEO_SHAPE.id(), Set.of(ObjectType.ID)),
        entry(ObjectType.ID, Set.of(GEO_SHAPE.id(), JsonType.ID)),
        entry(ArrayType.ID, Set.of()), // convertability handled in ArrayType
        entry(BitStringType.ID, Set.of(BitStringType.ID)),
        entry(JsonType.ID, Set.of(ObjectType.ID))
    );

    /**
     * Contains number conversions which are "safe" (= a conversion would not reduce the number of bytes
     * used to store the value)
     */
    private static final Map<Integer, Set<DataType<?>>> SAFE_CONVERSIONS = Map.of(
        BYTE.id(), Set.of(SHORT, INTEGER, LONG, TIMESTAMPZ, TIMESTAMP, DATE, FLOAT, DOUBLE),
        SHORT.id(), Set.of(INTEGER, LONG, TIMESTAMPZ, TIMESTAMP, DATE, FLOAT, DOUBLE),
        INTEGER.id(), Set.of(LONG, TIMESTAMPZ, TIMESTAMP, DATE, FLOAT, DOUBLE),
        LONG.id(), Set.of(TIMESTAMPZ, TIMESTAMP, DATE, DOUBLE),
        FLOAT.id(), Set.of(DOUBLE));

    public static boolean isArray(DataType<?> type) {
        return type.id() == ArrayType.ID;
    }

    public static List<DataType<?>> listFromStream(StreamInput in) throws IOException {
        return in.readList(DataTypes::fromStream);
    }

    public static DataType<?> fromStream(StreamInput in) throws IOException {
        int i = in.readVInt();
        try {
            return TYPE_REGISTRY.get(i).read(in);
        } catch (NullPointerException e) {
            Lazy.LOGGER.error(String.format(Locale.ENGLISH, "%d is missing in TYPE_REGISTRY", i), e);
            throw e;
        }
    }

    public static void toStream(Collection<? extends DataType<?>> types, StreamOutput out) throws IOException {
        out.writeVInt(types.size());
        for (DataType<?> type : types) {
            toStream(type, out);
        }
    }

    public static void toStream(DataType<?> type, StreamOutput out) throws IOException {
        out.writeVInt(type.id());
        type.writeTo(out);
    }

    private static final Map<Class<?>, DataType<?>> POJO_TYPE_MAPPING = Map.ofEntries(
        entry(Double.class, DOUBLE),
        entry(Float.class, FLOAT),
        entry(Integer.class, INTEGER),
        entry(Long.class, LONG),
        entry(Short.class, SHORT),
        entry(Byte.class, BYTE),
        entry(Boolean.class, BOOLEAN),
        entry(Map.class, UNTYPED_OBJECT),
        entry(String.class, STRING),
        entry(BytesRef.class, STRING),
        entry(PointImpl.class, GEO_POINT),
        entry(JtsPoint.class, GEO_POINT),
        entry(Character.class, STRING),
        entry(BitString.class, BitStringType.INSTANCE_ONE),
        entry(TimeTZ.class, TimeTZType.INSTANCE)
    );

    @SuppressWarnings({"unchecked"})
    public static DataType<?> guessType(Object value) {
        if (value == null) {
            return UNDEFINED;
        } else if (value instanceof Map) {
            return UNTYPED_OBJECT;
        } else if (value instanceof List list) {
            return valueFromList(list);
        } else if (value instanceof Object[] objectArray) {
            return valueFromList(Arrays.asList(objectArray));
        } else if (value instanceof float[] values) {
            return new FloatVectorType(values.length);
        }
        DataType<?> dataType = POJO_TYPE_MAPPING.get(value.getClass());
        if (dataType == null) {
            throw new IllegalArgumentException("Cannot detect the type of the value: " + value);
        }
        return dataType;
    }

    /**
     * @return Returns the closest integral type for a numeric type or null
     */
    @Nullable
    public static DataType<?> getIntegralReturnType(DataType<?> argumentType) {
        switch (argumentType.id()) {
            case ByteType.ID:
            case ShortType.ID:
            case IntegerType.ID:
            case FloatType.ID:
                return DataTypes.INTEGER;

            case DoubleType.ID:
            case LongType.ID:
                return DataTypes.LONG;

            default:
                return null;
        }
    }

    private static DataType<?> valueFromList(List<Object> value) {
        DataType<?> highest = DataTypes.UNDEFINED;
        for (Object o : value) {
            if (o == null) {
                continue;
            }
            DataType<?> current = guessType(o);
            // JSON libraries tend to optimize things like [ 0.0, 1.2 ] to [ 0, 1.2 ]; so we allow mixed types
            // in such cases.
            if (!current.equals(highest) && !safeConversionPossible(current, highest)) {
                throw new IllegalArgumentException(
                    "Mixed dataTypes inside a list are not supported. Found " + highest + " and " + current);
            }
            if (current.precedes(highest)) {
                highest = current;
            }
        }
        return new ArrayType<>(highest);
    }

    private static boolean safeConversionPossible(DataType<?> type1, DataType<?> type2) {
        final DataType<?> source;
        final DataType<?> target;
        if (type1.precedes(type2)) {
            source = type2;
            target = type1;
        } else {
            source = type1;
            target = type2;
        }
        if (source.id() == DataTypes.UNDEFINED.id()) {
            return true;
        }
        Set<DataType<?>> conversions = SAFE_CONVERSIONS.get(source.id());
        return conversions != null && conversions.contains(target);
    }

    public static final Map<String, DataType<?>> TYPES_BY_NAME_OR_ALIAS = Map.ofEntries(
        entry(UNDEFINED.getName(), UNDEFINED),
        entry(BYTE.getName(), BYTE),
        entry(BOOLEAN.getName(), BOOLEAN),
        entry(STRING.getName(), STRING),
        entry(CHARACTER.getName(), CHARACTER),
        entry(IP.getName(), IP),
        entry(DOUBLE.getName(), DOUBLE),
        entry(FLOAT.getName(), FLOAT),
        entry(SHORT.getName(), SHORT),
        entry(INTEGER.getName(), INTEGER),
        entry(LONG.getName(), LONG),
        entry(NUMERIC.getName(), NUMERIC),
        entry(RowType.EMPTY.getName(), RowType.EMPTY),
        entry(TIMETZ.getName(), TIMETZ),
        entry(TIMESTAMPZ.getName(), TIMESTAMPZ),
        entry(TIMESTAMP.getName(), TIMESTAMP),
        entry(ObjectType.NAME, UNTYPED_OBJECT),
        entry(GEO_POINT.getName(), GEO_POINT),
        entry(GEO_SHAPE.getName(), GEO_SHAPE),
        entry(REGPROC.getName(), REGPROC),
        entry(REGCLASS.getName(), REGCLASS),
        entry(OIDVECTOR.getName(), OIDVECTOR),
        entry("int2", SHORT),
        entry("int", INTEGER),
        entry("int4", INTEGER),
        entry("int8", LONG),
        entry("name", STRING),
        entry("long", LONG),
        entry("\"char\"", BYTE),
        entry("short", SHORT),
        entry("float", FLOAT),
        entry("float4", FLOAT),
        entry("float8", DOUBLE),
        entry("double", DOUBLE),
        entry("string", STRING),
        entry("varchar", STRING),
        entry("character varying", STRING),
        entry("char", CHARACTER),
        entry("timetz", TIMETZ),
        entry("timestamptz", TIMESTAMPZ),
        entry("timestamp", TIMESTAMP),
        entry("interval", INTERVAL),
        entry(DATE.getName(), DATE),
        entry(BitStringType.INSTANCE_ONE.getName(), BitStringType.INSTANCE_ONE),
        entry(JsonType.INSTANCE.getName(), JsonType.INSTANCE),
        entry("decimal", NUMERIC),
        entry(FloatVectorType.INSTANCE_ONE.getName(), FloatVectorType.INSTANCE_ONE)
    );

    public static DataType<?> ofName(String typeName) {
        DataType<?> dataType = ofNameOrNull(typeName);
        if (dataType == null) {
            throw new IllegalArgumentException("Cannot find data type: " + typeName);
        }
        return dataType;
    }

    public static DataType<?> of(String typeName, List<Integer> parameters) {
        if (typeName.equalsIgnoreCase(BitStringType.NAME)) {
            return new BitStringType(
                parameters.isEmpty() ? BitStringType.DEFAULT_LENGTH : parameters.get(0));
        }
        DataType<?> dataType = ofNameOrNull(typeName);
        if (dataType == null) {
            throw new IllegalArgumentException("Cannot find data type: " + typeName);
        }
        if (!parameters.isEmpty()) {
            return switch (dataType.id()) {
                case StringType.ID -> StringType.of(parameters);
                case CharacterType.ID -> CharacterType.of(parameters);
                case NumericType.ID -> NumericType.of(parameters);
                case FloatVectorType.ID -> new FloatVectorType(parameters.get(0));
                default -> throw new IllegalArgumentException(
                    "The '" + typeName + "' type doesn't support type parameters.");
            };
        } else {
            return dataType;
        }
    }

    @Nullable
    public static DataType<?> ofNameOrNull(String typeName) {
        return TYPES_BY_NAME_OR_ALIAS.get(typeName);
    }

    private static final Map<String, DataType<?>> MAPPING_NAMES_TO_TYPES = Map.ofEntries(
        entry("date", DataTypes.TIMESTAMPZ),
        entry("string", DataTypes.STRING),
        entry("keyword", DataTypes.STRING),
        entry("text", DataTypes.STRING),
        entry("boolean", DataTypes.BOOLEAN),
        entry("byte", DataTypes.BYTE),
        entry("short", DataTypes.SHORT),
        entry("integer", DataTypes.INTEGER),
        entry("long", DataTypes.LONG),
        entry("float", DataTypes.FLOAT),
        entry("double", DataTypes.DOUBLE),
        entry("ip", DataTypes.IP),
        entry("geo_point", DataTypes.GEO_POINT),
        entry("geo_shape", DataTypes.GEO_SHAPE),
        entry("object", UNTYPED_OBJECT),
        entry("nested", UNTYPED_OBJECT),
        entry("interval", DataTypes.INTERVAL),
        entry(FloatVectorType.INSTANCE_ONE.getName(), FloatVectorType.INSTANCE_ONE)
    );

    private static final Map<Integer, String> TYPE_IDS_TO_MAPPINGS = Map.ofEntries(
        entry(TIMESTAMPZ.id(), "date"),
        entry(TIMESTAMP.id(), "date"),
        entry(STRING.id(), "keyword"),
        entry(CHARACTER.id(), "keyword"),
        entry(BYTE.id(), "byte"),
        entry(BOOLEAN.id(), "boolean"),
        entry(IP.id(), "ip"),
        entry(DOUBLE.id(), "double"),
        entry(FLOAT.id(), "float"),
        entry(SHORT.id(), "short"),
        entry(INTEGER.id(), "integer"),
        entry(LONG.id(), "long"),
        entry(ObjectType.ID, "object"),
        entry(GEO_SHAPE.id(), "geo_shape"),
        entry(GEO_POINT.id(), "geo_point"),
        entry(INTERVAL.id(), "interval"),
        entry(BitStringType.ID, "bit"),
        entry(FloatVectorType.ID, FloatVectorType.INSTANCE_ONE.getName())
    );

    @Nullable
    public static String esMappingNameFrom(int typeId) {
        return TYPE_IDS_TO_MAPPINGS.get(typeId);
    }

    @Nullable
    public static DataType<?> ofMappingName(String name) {
        return MAPPING_NAMES_TO_TYPES.get(name);
    }

    /**
     * Checks if the {@link DataType} is a primitive data type.
     * The parameters of the data type are ignored.
     */
    public static boolean isPrimitive(DataType<?> type) {
        return PRIMITIVE_TYPE_IDS.contains(type.id());
    }

    /**
     * Checks if the {@link DataType} is a numeric primitive data type.
     * The parameters of the data type are ignored.
     */
    public static boolean isNumericPrimitive(DataType<?> type) {
        return NUMERIC_PRIMITIVE_TYPE_IDS.contains(type.id());
    }

    /**
     * Register a custom data type to the type registry.
     *
     * <p>Note: If registering is done inside a static block, be sure the class is loaded initially.
     * Otherwise it might not be registered on all nodes.
     * </p>
     */
    public static void register(int id, Writeable.Reader<DataType<?>> dataType) {
        if (TYPE_REGISTRY.put(id, dataType) != null) {
            throw new IllegalArgumentException("Already got a dataType with id " + id);
        }
    }

    public static Streamer<?>[] getStreamers(Collection<? extends DataType<?>> dataTypes) {
        Streamer<?>[] streamer = new Streamer[dataTypes.size()];
        int idx = 0;
        for (DataType<?> dataType : dataTypes) {
            streamer[idx] = dataType.streamer();
            idx++;
        }
        return streamer;
    }

    /**
     * Compares any two {@link DataType} by their IDs and names. For ObjectTypes, their inner types are compared only if the names are the same.
     * The rest of the DataTypes are compared by their IDs.
     * The parameters of the data types, if they have any, are ignored.
     */
    public static boolean isCompatibleType(DataType<?> left, DataType<?> right) {
        if (left.id() != right.id()) {
            return false;
        } else if (isArray(left)) {
            return isCompatibleType(
                ((ArrayType<?>) left).innerType(),
                ((ArrayType<?>) right).innerType());
        } else if (left.id() == ObjectType.ID) {
            var l = (ObjectType) left;
            var r = (ObjectType) right;
            for (var lEntry : l.innerTypes().entrySet()) {
                var lInner = lEntry.getValue();
                var rInner = r.innerTypes().get(lEntry.getKey());
                if (rInner == null) {  // skip if the names are different
                    continue;
                }
                if (!DataTypes.isCompatibleType(lInner, rInner)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Compares two {@link List<DataType>} by their IDs and names. For ObjectTypes, their inner types are compared only if the names are the same.
     * The rest of the DataTypes are compared by their IDs.
     * The parameters of the data types, if they have any, are ignored.
     */
    public static boolean isCompatibleType(List<DataType<?>> left, List<DataType<?>> right) {
        if (left.size() != right.size()) {
            return false;
        }
        assert left instanceof RandomAccess && right instanceof RandomAccess
            : "data type lists should support RandomAccess for fast lookups";
        for (int i = 0; i < left.size(); i++) {
            if (!isCompatibleType(left.get(i), right.get(i))) {
                return false;
            }
        }
        return true;
    }

    public static DataType<?> merge(DataType<?> leftType, DataType<?> rightType) {
        DataType<?> type;
        if (leftType.id() == ObjectType.ID && rightType.id() == ObjectType.ID) {
            type = ObjectType.merge((ObjectType) leftType, (ObjectType) rightType);
        } else if (leftType.id() == ArrayType.ID && rightType.id() == ArrayType.ID) {
            type = new ArrayType<>(merge(((ArrayType<?>) leftType).innerType(), ((ArrayType<?>) rightType).innerType()));
        } else {
            if (leftType.precedes(rightType)) {
                if (rightType.isConvertableTo(leftType, false)) {
                    return leftType;
                }
                throw new IllegalArgumentException("'" + rightType + "' is not convertible to '" + leftType + "'");
            } else {
                if (leftType.isConvertableTo(rightType, false)) {
                    return rightType;
                }
                throw new IllegalArgumentException("'" + leftType + "' is not convertible to '" + rightType + "'");
            }
        }
        return type;
    }

    public static DataType<?> fromId(Integer id) {
        return TYPES_BY_NAME_OR_ALIAS.values().stream()
            .filter(x -> x.id() == id)
            .findFirst()
            .orElse(DataTypes.UNDEFINED);
    }
}
