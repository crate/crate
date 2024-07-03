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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.EnumSets;
import io.crate.common.collections.Lists;
import io.crate.common.collections.Sets;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Scalar;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

public final class Signature implements Writeable, Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Signature.class);

    public static Builder builder(FunctionName name, FunctionType type) {
        return new Builder()
            .name(name)
            .kind(type);
    }

    public static Builder builder(String name, FunctionType type) {
        return new Builder()
            .name(name)
            .kind(type);
    }

    public static Builder builder(Signature signature) {
        return new Builder(signature);
    }

    public static class Builder {
        private FunctionName name;
        private FunctionType kind;
        private List<TypeSignature> argumentTypes = Collections.emptyList();
        private TypeSignature returnType;
        private Set<Scalar.Feature> features = Set.of();
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

        public Builder feature(Scalar.Feature feature) {
            this.features = Sets.concat(this.features, feature);
            return this;
        }

        public Builder features(Set<Scalar.Feature> features) {
            this.features = Sets.union(this.features, features);
            return this;
        }

        public Builder features(Scalar.Feature feature, Scalar.Feature ... rest) {
            this.features = EnumSet.of(feature, rest);
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

    /**
     * Create a signature out of the read values of the old FunctionInfo format for BWC compatibility with
     * nodes < 4.2.0.
     */
    public static Signature readFromFunctionInfo(StreamInput in) throws IOException {
        // read old FunctionIdent
        var functionName = new FunctionName(in);
        int numTypes = in.readVInt();
        ArrayList<TypeSignature> argumentTypeSignatures = new ArrayList<>(numTypes);
        for (int i = 0; i < numTypes; i++) {
            argumentTypeSignatures.add(DataTypes.fromStream(in).getTypeSignature());
        }
        // FunctionIdent end

        var returnType = DataTypes.fromStream(in);
        var type = FunctionType.values()[in.readVInt()];

        int enumElements = in.readVInt();
        var features = Collections.unmodifiableSet(EnumSets.unpackFromInt(enumElements, Scalar.Feature.class));

        return Signature.builder(functionName, type)
            .argumentTypes(argumentTypeSignatures)
            .returnType(returnType.getTypeSignature())
            .features(features)
            .build();
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

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE
            + name.ramBytesUsed()
            + argumentTypes.stream().mapToLong(TypeSignature::ramBytesUsed).sum()
            + returnType.ramBytesUsed();
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
        return Lists.map(argumentTypes, TypeSignature::createType);
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
        return o instanceof Signature signature &&
            name.equals(signature.name) &&
            kind == signature.kind &&
            argumentTypes.equals(signature.argumentTypes) &&
            returnType.equals(signature.returnType);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + kind.hashCode();
        result = 31 * result + argumentTypes.hashCode();
        result = 31 * result + returnType.hashCode();
        return result;
    }

    @Override
    public String toString() {
        List<String> allConstraints = List.of();
        if (bindingInfo != null) {
            allConstraints = Lists.map(bindingInfo.getTypeVariableConstraints(), TypeVariableConstraint::toString);
        }

        return name + (allConstraints.isEmpty() ? "" : "<" + String.join(",", allConstraints) + ">") +
               "(" + Lists.joinOn(",", argumentTypes, TypeSignature::toString) + "):" + returnType;
    }


    /**
     * Write the given {@link Signature} to the stream in the format of the old FunctionInfo class for BWC compatibility
     * with nodes < 4.2.0
     * @param argumentDataTypes is list of concrete types when getArgumentDataTypes() cannot be used as it's contains type variable constraints.
     */
    public void writeAsFunctionInfo(StreamOutput out, List<DataType<?>> argumentDataTypes) throws IOException {

        // old FunctionIdent
        name.writeTo(out);

        out.writeVInt(argumentDataTypes.size());
        for (DataType<?> argumentType : argumentDataTypes) {
            DataTypes.toStream(argumentType, out);
        }
        // FunctionIdent end

        DataTypes.toStream(returnType.createType(), out);
        out.writeVInt(kind.ordinal());
        out.writeVInt(EnumSets.packToInt(features));
    }
}
