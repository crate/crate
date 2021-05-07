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

package io.crate.metadata.functions;

import io.crate.common.collections.EnumSets;
import io.crate.common.collections.Lists2;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Scalar;
import io.crate.types.DataType;
import io.crate.types.TypeSignature;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public final class Signature implements Writeable {

    /**
     * See {@link #aggregate(FunctionName, TypeSignature...)}
     */
    public static Signature aggregate(String name, TypeSignature... types) {
        return aggregate(new FunctionName(null, name), types);
    }

    /**
     * Shortcut for creating a signature of type {@link FunctionType#AGGREGATE}.
     * The last element of the given types is handled as the return type.
     *
     * @param name      The fqn function name.
     * @param types     The argument and return (last element) types
     * @return          The created signature
     */
    public static Signature aggregate(FunctionName name, TypeSignature... types) {
        return signatureBuilder(name, FunctionType.AGGREGATE, types).build();
    }

    /**
     * See {@link #scalar(FunctionName, TypeSignature...)}
     */
    public static Signature scalar(String name, TypeSignature... types) {
        return scalar(new FunctionName(null, name), types);
    }

    /**
     * Shortcut for creating a signature of type {@link FunctionType#TABLE}.
     * The last element of the given types is handled as the return type.
     *
     * @param name      The fqn function name.
     * @param types     The argument and return (last element) types
     * @return          The created signature
     */
    public static Signature table(FunctionName name, TypeSignature... types) {
        return signatureBuilder(name, FunctionType.TABLE, types).build();
    }

    /**
     * See {@link #table(FunctionName, TypeSignature...)}
     */
    public static Signature table(String name, TypeSignature... types) {
        return table(new FunctionName(null, name), types);
    }

    /**
     * Shortcut for creating a signature of type {@link FunctionType#WINDOW}.
     * The last element of the given types is handled as the return type.
     *
     * @param name      The fqn function name.
     * @param types     The argument and return (last element) types
     * @return          The created signature
     */
    public static Signature window(FunctionName name, TypeSignature... types) {
        return signatureBuilder(name, FunctionType.WINDOW, types).build();
    }

    /**
     * See {@link #window(FunctionName, TypeSignature...)}
     */
    public static Signature window(String name, TypeSignature... types) {
        return window(new FunctionName(null, name), types);
    }

    /**
     * Shortcut for creating a signature of type {@link FunctionType#SCALAR}.
     * The last element of the given types is handled as the return type.
     *
     * @param name      The fqn function name.
     * @param types     The argument and return (last element) types
     * @return          The created signature
     */
    public static Signature scalar(FunctionName name, TypeSignature... types) {
        return signatureBuilder(name, FunctionType.SCALAR, types).build();
    }

    private static Signature.Builder signatureBuilder(FunctionName name, FunctionType type, TypeSignature... types) {
        assert types.length > 0 : "Types must contain at least the return type (last element), 0 types given";
        Builder builder = Signature.builder()
            .name(name)
            .kind(type)
            .returnType(types[types.length - 1]);
        if (types.length > 1) {
            builder.argumentTypes(Arrays.copyOf(types, types.length - 1));
        }
        return builder;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Signature signature) {
        return new Builder(signature);
    }

    public static class Builder {
        private FunctionName name;
        private FunctionType kind;
        private List<TypeSignature> argumentTypes = Collections.emptyList();
        private TypeSignature returnType;
        private Set<Scalar.Feature> features = Scalar.DETERMINISTIC_ONLY;
        private List<TypeVariableConstraint> typeVariableConstraints = Collections.emptyList();
        private List<TypeSignature> variableArityGroup = Collections.emptyList();
        private boolean variableArity = false;
        private boolean allowCoercion = true;

        public Builder() {
        }

        public Builder(Signature signature) {
            name = signature.getName();
            kind = signature.getKind();
            argumentTypes = signature.getArgumentTypes();
            returnType = signature.getReturnType();
            features = signature.getFeatures();
            if (signature.getBindingInfo() != null) {
                typeVariableConstraints = signature.getBindingInfo().getTypeVariableConstraints();
                variableArityGroup = signature.getBindingInfo().getVariableArityGroup();
                variableArity = signature.getBindingInfo().isVariableArity();
                allowCoercion = signature.getBindingInfo().isCoercionAllowed();
            }
        }

        public Builder name(String name) {
            return name(new FunctionName(null, name));
        }

        public Builder name(FunctionName name) {
            this.name = name;
            return this;
        }

        public Builder kind(FunctionType kind) {
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

        public Builder features(Set<Scalar.Feature> features) {
            this.features = features;
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
                features,
                variableArityGroup,
                variableArity,
                allowCoercion);
        }
    }


    private final FunctionName name;
    private final FunctionType kind;
    private final List<TypeSignature> argumentTypes;
    private final TypeSignature returnType;
    private final Set<Scalar.Feature> features;
    @Nullable
    private final SignatureBindingInfo bindingInfo;

    private Signature(FunctionName name,
                      FunctionType kind,
                      List<TypeVariableConstraint> typeVariableConstraints,
                      List<TypeSignature> argumentTypes,
                      TypeSignature returnType,
                      Set<Scalar.Feature> features,
                      List<TypeSignature> variableArityGroup,
                      boolean variableArity,
                      boolean allowCoercion) {
        this.name = name;
        this.kind = kind;
        this.argumentTypes = argumentTypes;
        this.returnType = returnType;
        this.features = features;
        this.bindingInfo = new SignatureBindingInfo(
            typeVariableConstraints,
            variableArityGroup,
            variableArity,
            allowCoercion
        );
    }

    public Signature(StreamInput in) throws IOException {
        name = new FunctionName(in);
        kind = FunctionType.values()[in.readVInt()];
        int argsSize = in.readVInt();
        argumentTypes = new ArrayList<>(argsSize);
        for (int i = 0; i < argsSize; i++) {
            argumentTypes.add(TypeSignature.fromStream(in));
        }
        returnType = TypeSignature.fromStream(in);
        int enumElements = in.readVInt();
        features = Collections.unmodifiableSet(EnumSets.unpackFromInt(enumElements, Scalar.Feature.class));
        bindingInfo = null;
    }

    public Signature withTypeVariableConstraints(TypeVariableConstraint... typeVariableConstraints) {
        return Signature.builder(this)
            .typeVariableConstraints(typeVariableConstraints)
            .build();
    }

    public Signature withVariableArity() {
        return Signature.builder(this)
            .setVariableArity(true)
            .build();
    }

    /*
     * Forbid coercion of argument types.
     * This prevents e.g. matching a numeric_only function with convertible argument (text).
     */
    public Signature withForbiddenCoercion() {
        return Signature.builder(this)
            .forbidCoercion()
            .build();
    }

    public Signature withFeatures(Set<Scalar.Feature> features) {
        return Signature.builder(this)
            .features(features)
            .build();
    }

    public FunctionName getName() {
        return name;
    }

    public FunctionType getKind() {
        return kind;
    }

    public List<TypeSignature> getArgumentTypes() {
        return argumentTypes;
    }

    public List<DataType<?>> getArgumentDataTypes() {
        return Lists2.map(argumentTypes, TypeSignature::createType);
    }

    public TypeSignature getReturnType() {
        return returnType;
    }

    public Set<Scalar.Feature> getFeatures() {
        return features;
    }

    public boolean hasFeature(Scalar.Feature feature) {
        return features.contains(feature);
    }

    public boolean isDeterministic() {
        return hasFeature(Scalar.Feature.DETERMINISTIC);
    }

    @Nullable
    public SignatureBindingInfo getBindingInfo() {
        return bindingInfo;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        name.writeTo(out);
        out.writeVInt(kind.ordinal());
        out.writeVInt(argumentTypes.size());
        for (TypeSignature typeSignature : argumentTypes) {
            TypeSignature.toStream(typeSignature, out);
        }
        TypeSignature.toStream(returnType, out);
        out.writeVInt(EnumSets.packToInt(features));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Signature signature = (Signature) o;
        return name.equals(signature.name) &&
               kind == signature.kind &&
               argumentTypes.equals(signature.argumentTypes) &&
               returnType.equals(signature.returnType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, kind, argumentTypes, returnType);
    }

    @Override
    public String toString() {
        List<String> allConstraints = List.of();
        if (bindingInfo != null) {
            allConstraints = Lists2.map(bindingInfo.getTypeVariableConstraints(), TypeVariableConstraint::toString);
        }

        return name + (allConstraints.isEmpty() ? "" : "<" + String.join(",", allConstraints) + ">") +
               "(" + Lists2.joinOn(",", argumentTypes, TypeSignature::toString) + "):" + returnType;
    }
}
