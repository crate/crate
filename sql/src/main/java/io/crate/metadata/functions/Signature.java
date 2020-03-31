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

import io.crate.common.collections.Lists2;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionName;
import io.crate.types.TypeSignature;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class Signature {

    /**
     * See {@link #scalar(FunctionName, TypeSignature...)}
     */
    public static Signature scalar(String name, TypeSignature... types) {
        return scalar(new FunctionName(null, name), types);
    }

    /**
     * Shortcut for creating a signature of type {@link FunctionInfo.Type#SCALAR}.
     * The last element of the given types is handled as the return type.
     *
     * @param name      The fqn function name.
     * @param types     The argument and return (last element) types
     * @return          The created signature
     */
    public static Signature scalar(FunctionName name, TypeSignature... types) {
        return scalarBuilder(name, types).build();
    }

    /**
     * Same as {@link #scalar(String, TypeSignature...)}, but don't allow coercion.
     * This prevents e.g. matching a numeric_only function with convertible argument (text).
     */
    public static Signature scalarWithForbiddenCoercion(String name, TypeSignature... types) {
        return scalarBuilder(new FunctionName(null, name), types)
            .forbidCoercion()
            .build();
    }

    private static Signature.Builder scalarBuilder(FunctionName name, TypeSignature... types) {
        assert types.length > 0 : "Types must contain at least the return type (last element), 0 types given";
        Builder builder = Signature.builder()
            .name(name)
            .kind(FunctionInfo.Type.SCALAR)
            .returnType(types[types.length - 1]);
        if (types.length > 1) {
            builder.argumentTypes(Arrays.copyOf(types, types.length - 1));
        }
        return builder;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private FunctionName name;
        private FunctionInfo.Type kind;
        private List<TypeSignature> argumentTypes = Collections.emptyList();
        private TypeSignature returnType;
        private List<TypeVariableConstraint> typeVariableConstraints = Collections.emptyList();
        private List<TypeSignature> variableArityGroup = Collections.emptyList();
        private boolean variableArity = false;
        private boolean allowCoercion = true;

        public Builder name(String name) {
            return name(new FunctionName(null, name));
        }

        public Builder name(FunctionName name) {
            this.name = name;
            return this;
        }

        public Builder kind(FunctionInfo.Type kind) {
            this.kind = kind;
            return this;
        }

        public Builder argumentTypes(TypeSignature... argumentTypes) {
            return argumentTypes(List.of(argumentTypes));
        }

        public Builder argumentTypes(List<TypeSignature> argumentTypes) {
            this.argumentTypes = argumentTypes;
            return this;
        }

        public Builder returnType(TypeSignature returnType) {
            this.returnType = returnType;
            return this;
        }

        public Builder typeVariableConstraints(TypeVariableConstraint... typeVariableConstraints) {
            return typeVariableConstraints(List.of(typeVariableConstraints));
        }

        public Builder typeVariableConstraints(List<TypeVariableConstraint> typeVariableConstraints) {
            this.typeVariableConstraints = typeVariableConstraints;
            return this;
        }

        public Builder variableArityGroup(List<TypeSignature> variableArityGroup) {
            this.variableArityGroup = variableArityGroup;
            this.variableArity = !variableArityGroup.isEmpty();
            return this;
        }

        public Builder setVariableArity(boolean variableArity) {
            this.variableArity = variableArity;
            return this;
        }

        public Builder forbidCoercion() {
            allowCoercion = false;
            return this;
        }

        public Signature build() {
            assert name != null : "Signature requires the 'name' to be set";
            assert kind != null : "Signature requires the 'kind' to be set";
            assert returnType != null : "Signature requires the 'returnType' to be set";
            return new Signature(
                name,
                kind,
                typeVariableConstraints,
                argumentTypes,
                returnType,
                variableArityGroup,
                variableArity,
                allowCoercion);
        }
    }


    private final FunctionName name;
    private final FunctionInfo.Type kind;
    private final List<TypeSignature> argumentTypes;
    private final TypeSignature returnType;
    private final List<TypeVariableConstraint> typeVariableConstraints;
    private final List<TypeSignature> variableArityGroup;
    private final boolean variableArity;
    private final boolean allowCoercion;

    private Signature(FunctionName name,
                      FunctionInfo.Type kind,
                      List<TypeVariableConstraint> typeVariableConstraints,
                      List<TypeSignature> argumentTypes,
                      TypeSignature returnType,
                      List<TypeSignature> variableArityGroup,
                      boolean variableArity,
                      boolean allowCoercion) {
        this.name = name;
        this.kind = kind;
        this.argumentTypes = argumentTypes;
        this.typeVariableConstraints = typeVariableConstraints;
        this.returnType = returnType;
        this.variableArityGroup = variableArityGroup;
        this.variableArity = variableArity;
        this.allowCoercion = allowCoercion;
    }

    public FunctionName getName() {
        return name;
    }

    public FunctionInfo.Type getKind() {
        return kind;
    }

    public List<TypeSignature> getArgumentTypes() {
        return argumentTypes;
    }

    public TypeSignature getReturnType() {
        return returnType;
    }

    public List<TypeVariableConstraint> getTypeVariableConstraints() {
        return typeVariableConstraints;
    }

    public List<TypeSignature> getVariableArityGroup() {
        return variableArityGroup;
    }

    public boolean isVariableArity() {
        return variableArity;
    }

    public boolean isCoercionAllowed() {
        return allowCoercion;
    }

    @Override
    public String toString() {
        List<String> allConstraints = Lists2.map(typeVariableConstraints, TypeVariableConstraint::toString);

        return name + (allConstraints.isEmpty() ? "" : "<" + String.join(",", allConstraints) + ">") +
               "(" + Lists2.joinOn(",", argumentTypes, TypeSignature::toString) + "):" + returnType;
    }
}
