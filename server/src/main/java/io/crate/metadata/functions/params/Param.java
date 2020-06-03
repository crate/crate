/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.functions.params;

import io.crate.exceptions.ConversionException;
import io.crate.expression.symbol.FuncArg;
import io.crate.types.ArrayType;
import io.crate.types.BooleanType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.StringType;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A single function parameter as part of a function parameter definition in {@link FuncParams}.
 * The class takes care of type checking and does type inference by converting types if possible.
 *
 * Parameters are meant to be passed by reference to denote a reoccurring parameter type.
 * For example, for a function add(x,y) we want x,y to match. That's why we define a single
 * Param which we use for both types, e.g.
 * {@code
 *  Param numParam = Param.of(NUMERIC);
 *  FuncParams.of(numParam, numParam);
 * }
 *
 * Every time an argument is assigned to a parameter, the DataType associated with this parameter
 * may change. To bind an argument, use the {@code bind(arg)} method. If the Parameter hasn't been
 * bound before, it is assigned a type; if the type is not allowed, we will try to convert the
 * argument's type to an accepted type in type precedence order.
 *
 * For example, for a Function func(a, b, a) when matching args (1::int, 'foo'::string, 2::long)
 * the state of each of the three parameters looks as follows:
 *
 * unbind() => (unbound, unbound, unbound)
 * bind(1::int) => (int, unbound, int)
 * bind('foo'::string) => (int, string, int)
 * bind(2::long) => (long, string, long)
 *
 */
public final class Param {

    public static final Param ANY = of();
    public static final Param NUMERIC = of(DataTypes.NUMERIC_PRIMITIVE_TYPES);
    public static final Param ANY_ARRAY = of(new ArrayType<>(DataTypes.UNDEFINED));
    public static final Param INTEGER = of(IntegerType.INSTANCE);
    public static final Param LONG = of(LongType.INSTANCE);
    public static final Param STRING = of(StringType.INSTANCE);
    public static final Param BOOLEAN = of(BooleanType.INSTANCE);

    /** A list of valid types which may be empty to allow any type */
    private final SortedSet<DataType> validTypes;
    /** A thread-safe reference to the current type information */
    private final ThreadLocal<FuncArg> boundType;
    /** A thread-safe reference to a stack which holds independent bindings */
    private final ThreadLocal<Deque<DataType>> multiBoundStack;

    @Nullable
    private final Param innerType;

    private Param(DataType... validTypes) {
        this(Collections.emptyList(), validTypes, null);
    }

    private Param(Collection<DataType> validTypes, DataType[] validTypes2, @Nullable Param innerType) {
        this.boundType = new ThreadLocal<>();
        this.validTypes = new TreeSet<>((o1, o2) -> {
            if (o1.precedes(o2)) {
                return -1;
            } else if (o2.precedes(o1)) {
                return 1;
            }
            return 0;
        });
        this.validTypes.addAll(validTypes);
        this.validTypes.addAll(Arrays.asList(validTypes2));
        this.multiBoundStack = ThreadLocal.withInitial(ArrayDeque::new);
        this.innerType = innerType;
    }

    /**
     * Creates a new Type with one or multiple valid {@link DataType}s.
     * @param validTypes The valid {@link DataType}s.
     * @return A new Type parameter.
     */
    public static Param of(DataType... validTypes) {
        return new Param(validTypes);
    }

    /**
     * Creates a new Type from a Collection and a list of {@link DataType}s.
     * @param validTypes The valid {@link DataType} as a Collection.
     * @param validTypes2 The valid {@link DataType}.
     * @return A new Type parameter.
     */
    public static Param of(Collection<DataType> validTypes, DataType... validTypes2) {
        return new Param(validTypes, validTypes2, null);
    }

    /**
     * Matches the inner Type of this Type.
     * @param innerType A Type to match against any inner type of a Type.
     * @return A new Type with an inner Type.
     */
    public Param withInnerType(Param innerType) {
        return new Param(this.validTypes, new DataType[]{}, innerType);
    }

    /**
     * Returns the valid {@link DataType}s of this Type.
     * @return A collection of valid types.
     */
    Collection<DataType> getValidTypes() {
        return validTypes;
    }

    DataType<?> getBoundType(boolean multiBind) {
        final DataType boundType;
        if (multiBind) {
            boundType = this.multiBoundStack.get().removeFirst();
        } else {
            boundType = this.boundType.get().valueType();
        }
        if (boundType == null) {
            throw new IllegalStateException("Type not bound when it should have been.");
        }
        if (this.innerType != null) {
            if (boundType instanceof ArrayType) {
                DataType<?> innerType = this.innerType.getBoundType(multiBind);
                if (innerType != null) {
                    return new ArrayType<>(innerType);
                }
            }
        }
        return boundType;
    }

    void bind(FuncArg funcArg, boolean multiBind) {
        Objects.requireNonNull(funcArg, "funcArg to bind must not be null");
        final FuncArg bound = this.boundType.get();
        final FuncArg updatedType;
        if (bound == null) {
            updatedType = bindFresh(funcArg, multiBind);
        } else {
            updatedType = rebind(bound, funcArg);
        }
        if (updatedType != bound) {
            if (multiBind) {
                this.multiBoundStack.get().addLast(updatedType.valueType());
            } else {
                this.boundType.set(updatedType);
            }
        }
    }

    private FuncArg bindFresh(FuncArg funcArg, boolean multiBind) {
        DataType argDataType = Objects.requireNonNull(funcArg.valueType(),
            "Provided argDataType type must not be null");
        if (!validTypes.isEmpty() && !validTypes.contains(argDataType)) {
            FuncArg convertedType = null;
            for (DataType targetType : validTypes) {
                convertedType = convert(funcArg, targetType);
                if (convertedType != null) {
                    break;
                }
            }
            if (convertedType == null) {
                throw new ConversionException(funcArg, validTypes);
            }
            return convertedType;
        } else if (innerType != null) {
            if (argDataType instanceof ArrayType) {
                DataType<?> innerType = Objects.requireNonNull(((ArrayType<?>) argDataType).innerType(),
                    "Inner type expected but no inner type for argument: " + funcArg);
                this.innerType.bind(new ConvertedArg(innerType, funcArg.canBeCasted(), funcArg.isValueSymbol()), multiBind);
            } else {
                throw new IllegalArgumentException("DataType with an inner type expected but not provided.");
            }
        }
        return funcArg;
    }

    private FuncArg rebind(FuncArg bound, FuncArg newTarget) {
        DataType<?> boundType = bound.valueType();
        DataType<?> targetType = Objects.requireNonNull(newTarget.valueType(), "Provided dataType type must not be null");
        if (boundType.equals(targetType)) {
            return bound;
        }
        FuncArg convertedType = convertTypes(newTarget, bound);
        if (convertedType == null) {
            throw new ConversionException(bound, targetType);
        }
        return convertedType;
    }

    private static FuncArg convert(FuncArg source, DataType target) {
        if (source.canBeCasted() && source.valueType().isConvertableTo(target, false)) {
            return new ConvertedArg(source, target);
        }
        return null;
    }

    private static class ConvertedArg implements FuncArg {

        private final DataType dataType;
        private final boolean canBeCasted;
        private final boolean isValueSymbol;

        private ConvertedArg(FuncArg sourceArg, DataType targetDataType) {
            if (!sourceArg.canBeCasted()) {
                throw new IllegalArgumentException("Converted argument must be castable.");
            }
            this.dataType = targetDataType;
            this.canBeCasted = true;
            this.isValueSymbol = sourceArg.isValueSymbol();
        }

        private ConvertedArg(DataType argumentType, boolean canBeCasted, boolean isValueSymbol) {
            this.dataType = argumentType;
            this.canBeCasted = canBeCasted;
            this.isValueSymbol = isValueSymbol;
        }

        @Override
        public DataType valueType() {
            return this.dataType;
        }

        @Override
        public boolean canBeCasted() {
            return canBeCasted;
        }

        @Override
        public boolean isValueSymbol() {
            return isValueSymbol;
        }

        @Override
        public String toString() {
            return dataType.toString();
        }
    }

    /**
     * Tries to convert two {@link DataType} by respecting the precedence if possible.
     * For example, if given type A and type B, where A has higher precedence,
     * first try to cast B to A. If that doesn't work, try casting A to B.
     * @param arg1 The first type given
     * @param arg2 The second type given
     * @return Either arg1 or arg2 depending on precedence and convertibility.
     */
    @Nullable
    private FuncArg convertTypes(FuncArg arg1, FuncArg arg2) {
        final FuncArg higherPrecedenceArg;
        final FuncArg lowerPrecedenceArg;
        if (arg1.valueType().precedes(arg2.valueType())) {
            higherPrecedenceArg = arg1;
            lowerPrecedenceArg = arg2;
        } else {
            higherPrecedenceArg = arg2;
            lowerPrecedenceArg = arg1;
        }

        final DataType<?> lowerPrecedenceType = lowerPrecedenceArg.valueType();
        final DataType<?> higherPrecedenceType = higherPrecedenceArg.valueType();

        final boolean lowerPrecedenceCastable = lowerPrecedenceType.isConvertableTo(higherPrecedenceType, false)
            && isTypeValid(higherPrecedenceType);
        final boolean higherPrecedenceCastable = higherPrecedenceType.isConvertableTo(lowerPrecedenceType, false)
            && isTypeValid(lowerPrecedenceType);

        if (lowerPrecedenceCastable) {
            return higherPrecedenceArg;
        } else if (higherPrecedenceCastable) {
            return lowerPrecedenceArg;
        }

        return null;
    }

    private boolean isTypeValid(DataType<?> dataType) {
        return validTypes.isEmpty() || validTypes.contains(dataType);
    }

    void unbind() {
        this.boundType.set(null);
        this.multiBoundStack.get().clear();
        if (innerType != null) {
            this.innerType.boundType.set(null);
        }
    }

}
