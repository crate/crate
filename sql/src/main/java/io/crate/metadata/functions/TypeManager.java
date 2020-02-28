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

package io.crate.metadata.functions;

import com.google.common.collect.ImmutableList;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.TypeSignature;
import io.crate.types.TypeSignatureParameter;
import io.crate.types.UndefinedType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.crate.types.TypeSignature.parseTypeSignature;

public class TypeManager {

    /**
     * Gets the type with the specified signature.
     */
    public DataType<?> getType(TypeSignature signature) {
        String base = signature.getBase();
        if (base.equalsIgnoreCase(ArrayType.NAME)) {
            List<TypeSignatureParameter> parameters = signature.getParameters();
            if (parameters.size() == 0) {
                return new ArrayType<>(UndefinedType.INSTANCE);
            }
            DataType<?> innerType = getType(parameters.get(0).getTypeSignature());
            return new ArrayType<>(innerType);
        }
        return DataTypes.ofName(signature.getBase());
    }

    /**
     * Gets the type with the specified base type, and the given parameters.
     */
    public DataType<?> getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters) {
        return getType(new TypeSignature(baseTypeName, typeParameters));
    }

    public Optional<DataType<?>> getCommonSuperType(DataType<?> firstType, DataType<?> secondType) {
        TypeCompatibility compatibility = compatibility(firstType, secondType);
        if (!compatibility.isCompatible()) {
            return Optional.empty();
        }
        return Optional.of(compatibility.getCommonSuperType());
    }

    public boolean canCoerce(DataType<?> fromType, DataType<?> toType) {
        return fromType.isConvertableTo(toType);
    }

    private TypeCompatibility compatibility(DataType<?> fromType, DataType<?> toType) {
        if (fromType.equals(toType)) {
            return TypeCompatibility.compatible(toType, true);
        }

        if (fromType.equals(UndefinedType.INSTANCE)) {
            return TypeCompatibility.compatible(toType, true);
        }

        if (toType.equals(UndefinedType.INSTANCE)) {
            return TypeCompatibility.compatible(fromType, false);
        }

        String fromTypeBaseName = fromType.getTypeSignature().getBase();
        String toTypeBaseName = toType.getTypeSignature().getBase();
        if (fromTypeBaseName.equals(toTypeBaseName)) {
            if (isCovariantParametrizedType(fromType)) {
                return typeCompatibilityForCovariantParametrizedType(fromType, toType);
            }
            return TypeCompatibility.compatible(fromType, false);
        }

        DataType<?> commonSuperType = convertType(fromType, toType);
        if (commonSuperType != null) {
            return TypeCompatibility.compatible(commonSuperType, commonSuperType.equals(toType));
        }
        Optional<DataType<?>> coercedType = coerceTypeBase(fromType, toType.getTypeSignature().getBase());
        if (coercedType.isPresent()) {
            return compatibility(coercedType.get(), toType);
        }

        coercedType = coerceTypeBase(toType, fromType.getTypeSignature().getBase());
        if (coercedType.isPresent()) {
            TypeCompatibility typeCompatibility = compatibility(fromType, coercedType.get());
            if (!typeCompatibility.isCompatible()) {
                return TypeCompatibility.incompatible();
            }
            return TypeCompatibility.compatible(typeCompatibility.getCommonSuperType(), false);
        }

        return TypeCompatibility.incompatible();
    }

    @Nullable
    private DataType<?> convertType(DataType<?> arg1, DataType<?> arg2) {
        final DataType<?> higherPrecedenceArg;
        final DataType<?> lowerPrecedenceArg;
        if (arg1.precedes(arg2)) {
            higherPrecedenceArg = arg1;
            lowerPrecedenceArg = arg2;
        } else {
            higherPrecedenceArg = arg2;
            lowerPrecedenceArg = arg1;
        }

        final boolean lowerPrecedenceCastable = lowerPrecedenceArg.isConvertableTo(higherPrecedenceArg);
        final boolean higherPrecedenceCastable = higherPrecedenceArg.isConvertableTo(lowerPrecedenceArg);

        // Check if one of the two arguments is a value symbol which can be converted easily, e.g. Literal
        // We also allow downcasts in this case because we can check during analyzing the statement if
        // the downcast succeeds.
        if (lowerPrecedenceCastable) {
            return higherPrecedenceArg;
        } else if (higherPrecedenceCastable) {
            return lowerPrecedenceArg;
        }

        return null;
    }

    private Optional<DataType<?>> coerceTypeBase(DataType<?> sourceType, String resultTypeBase) {
        DataType<?> resultType = getType(parseTypeSignature(resultTypeBase));
        if (resultType.equals(sourceType)) {
            return Optional.of(sourceType);
        }
        return Optional.ofNullable(convertType(sourceType, resultType));
    }

    private static boolean isCovariantParametrizedType(DataType<?> type) {
        // if we ever introduce contravariant, this function should be changed to return an enumeration: INVARIANT, COVARIANT, CONTRAVARIANT
        return type instanceof ObjectType || type instanceof ArrayType;
    }

    private TypeCompatibility typeCompatibilityForCovariantParametrizedType(DataType<?> fromType, DataType<?> toType) {
        checkState(fromType.getClass().equals(toType.getClass()));
        ImmutableList.Builder<TypeSignatureParameter> commonParameterTypes = ImmutableList.builder();
        List<DataType<?>> fromTypeParameters = fromType.getTypeParameters();
        List<DataType<?>> toTypeParameters = toType.getTypeParameters();

        if (fromTypeParameters.size() != toTypeParameters.size()) {
            return TypeCompatibility.incompatible();
        }

        boolean coercible = true;
        for (int i = 0; i < fromTypeParameters.size(); i++) {
            TypeCompatibility compatibility = compatibility(fromTypeParameters.get(i), toTypeParameters.get(i));
            if (!compatibility.isCompatible()) {
                return TypeCompatibility.incompatible();
            }
            coercible &= compatibility.isCoercible();
            commonParameterTypes.add(TypeSignatureParameter.of(compatibility.getCommonSuperType().getTypeSignature()));
        }
        String typeBase = fromType.getTypeSignature().getBase();
        return TypeCompatibility.compatible(getType(new TypeSignature(typeBase, commonParameterTypes.build())),
                                            coercible);
    }

    public static class TypeCompatibility {
        private final Optional<DataType<?>> commonSuperType;
        private final boolean coercible;

        // Do not call constructor directly. Use factory methods.
        private TypeCompatibility(Optional<DataType<?>> commonSuperType, boolean coercible) {
            // Assert that: coercible => commonSuperType.isPresent
            // The factory API is designed such that this is guaranteed.
            checkArgument(!coercible || commonSuperType.isPresent());

            this.commonSuperType = commonSuperType;
            this.coercible = coercible;
        }

        private static TypeCompatibility compatible(DataType<?> commonSuperType, boolean coercible) {
            return new TypeCompatibility(Optional.of(commonSuperType), coercible);
        }

        private static TypeCompatibility incompatible() {
            return new TypeCompatibility(Optional.empty(), false);
        }

        public boolean isCompatible() {
            return commonSuperType.isPresent();
        }

        public DataType<?> getCommonSuperType() {
            checkState(commonSuperType.isPresent(), "Types are not compatible");
            return commonSuperType.get();
        }

        public boolean isCoercible() {
            return coercible;
        }
    }
}
