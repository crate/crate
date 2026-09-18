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
import org.jspecify.annotations.Nullable;

import io.crate.common.collections.EnumSets;
import io.crate.common.collections.Lists;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionType;
import io.crate.types.DataType;
import io.crate.types.TypeSignature;

public final class Signature implements Writeable, Accountable {


    public enum Feature {
        DETERMINISTIC,
        /**
         * If this feature is set, for this function it is possible to replace the containing
         * comparison while preserving the truth value for all used operators
         * with or without an operator mapping.
         * <p>
         * It describes the following:
         * <p>
         * say we have a comparison-query like this:
         * <p>
         * col > 10.5
         * <p>
         * then a function f, for which comparisons are replaceable, can be applied so
         * that:
         * <p>
         * f(col) > f(10.5)
         * <p>
         * for all col for which col > 10.5 is true. Maybe > needs to be mapped to another
         * operator, but this may not depend on the actual values used here.
         * <p>
         * Fun facts:
         * <p>
         * * This property holds for the = comparison operator for all functions f.
         * * This property is transitive so if f and g are replaceable,
         * then f(g(x)) also is
         * * it is possible to replace:
         * <p>
         * col > 10.5
         * <p>
         * with:
         * <p>
         * f(col) > f(10.5)
         * <p>
         * for every operator (possibly mapped) and the query is still equivalent.
         * <p>
         * Example 1:
         * <p>
         * if f is defined as f(v) = v + 1
         * then col + 1 > 11.5 must be true for all col > 10.5.
         * This is indeed true.
         * <p>
         * So f is replaceable for >.
         * <p>
         * Fun fact: for f all comparison operators =, >, <, >=,<= are replaceable
         * <p>
         * Example 2 (a rounding function):
         * <p>
         * if f is defined as f(v) = ceil(v)
         * then ceil(col) > 11 for all col > 10.5.
         * But there is 10.8 for which f is 11 and
         * 11 > 11 is false.
         * <p>
         * Here a simple mapping of the operator will do the trick:
         * <p>
         * > -> >=
         * < -> <=
         * <p>
         * So for f comparisons are replaceable using the mapping above.
         * <p>
         * Example 3:
         * <p>
         * if f is defined as f(v) = v % 5
         * then col % 5 > 0.5 for all col > 10.5
         * but there is 20 for which
         * f is 0 and
         * 0 > 0.5 is false.
         * <p>
         * So for f comparisons cannot be replaced.
         */
        COMPARISON_REPLACEMENT,
        LAZY_ATTRIBUTES,

        /**
         * Function returns null if and only if any argument is null
         * Note, that "if and only if" turns weaker implication arg null => f null
         * to a stronger equivalence: arg null <==> f null
         * <p>
         *
         * Not set for functions that return null under special conditions - e.g. null only if all
         * arguments are null, or if array argument has null, e.g 5 in [1, null] -> NULL.
         * </p>
         **/
        STRICTNULL,

        /**
         * Function never returns null.
         **/
        NOTNULL,

        /**
         * Aggregate function result depends on the input rows consumption order.
         **/
        ORDER_SENSITIVE
    }

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Signature.class);

    public static Builder builder(FunctionName name, FunctionType type) {
        return new Builder()
            .name(name)
            .type(type);
    }

    public static Builder builder(String name, FunctionType type) {
        return new Builder()
            .name(name)
            .type(type);
    }

    public static Builder builder(Signature signature) {
        return new Builder(signature);
    }

    public static class Builder {
        private FunctionName name;
        private FunctionType type;
        private List<TypeSignature> argumentTypes = Collections.emptyList();
        private TypeSignature returnType;
        private Set<Feature> features = Set.of();
        private List<TypeVariableConstraint> typeVariableConstraints = Collections.emptyList();
        private List<TypeSignature> variableArityGroup = Collections.emptyList();
        private boolean variableArity = false;
        private boolean allowCoercion = true;
        private boolean bindActualTypes = false;

        public Builder() {
        }

        public Builder(Signature signature) {
            name = signature.getName();
            type = signature.getType();
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

        public Builder type(FunctionType type) {
            this.type = type;
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

        public Builder features(Set<Signature.Feature> features) {
            this.features = features;
            return this;
        }

        public Builder features(Signature.Feature feature, Signature.Feature ... rest) {
            this.features = EnumSet.of(feature, rest);
            return this;
        }

        public Builder features(Signature.Feature feature) {
            this.features = EnumSet.of(feature);
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

        public Builder bindActualTypes() {
            bindActualTypes = true;
            return this;
        }

        public Signature build() {
            assert name != null : "Signature requires the 'name' to be set";
            assert type != null : "Signature requires the 'type' to be set";
            assert returnType != null : "Signature requires the 'returnType' to be set";
            return new Signature(
                name,
                type,
                typeVariableConstraints,
                argumentTypes,
                returnType,
                features,
                variableArityGroup,
                variableArity,
                allowCoercion,
                bindActualTypes);
        }
    }

    private final FunctionName name;
    private final FunctionType type;
    private final List<TypeSignature> argumentTypes;
    private final TypeSignature returnType;
    private final Set<Signature.Feature> features;
    @Nullable
    private final SignatureBindingInfo bindingInfo;

    private Signature(FunctionName name,
                      FunctionType type,
                      List<TypeVariableConstraint> typeVariableConstraints,
                      List<TypeSignature> argumentTypes,
                      TypeSignature returnType,
                      Set<Signature.Feature> features,
                      List<TypeSignature> variableArityGroup,
                      boolean variableArity,
                      boolean allowCoercion,
                      boolean bindActualTypes) {
        this.name = name;
        this.type = type;
        this.argumentTypes = argumentTypes;
        this.returnType = returnType;
        this.features = features;
        this.bindingInfo = new SignatureBindingInfo(
            typeVariableConstraints,
            variableArityGroup,
            variableArity,
            allowCoercion,
            bindActualTypes
        );
    }

    public Signature(StreamInput in) throws IOException {
        name = new FunctionName(in);
        type = FunctionType.values()[in.readVInt()];
        int argsSize = in.readVInt();
        argumentTypes = new ArrayList<>(argsSize);
        for (int i = 0; i < argsSize; i++) {
            argumentTypes.add(TypeSignature.fromStream(in));
        }
        returnType = TypeSignature.fromStream(in);
        int enumElements = in.readVInt();
        features = Collections.unmodifiableSet(EnumSets.unpackFromInt(enumElements, Feature.class));
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

    public FunctionType getType() {
        return type;
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

    public Set<Signature.Feature> getFeatures() {
        return features;
    }

    public boolean hasFeature(Signature.Feature feature) {
        return features.contains(feature);
    }

    public boolean isDeterministic() {
        return hasFeature(Feature.DETERMINISTIC);
    }

    @Nullable
    public SignatureBindingInfo getBindingInfo() {
        return bindingInfo;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        name.writeTo(out);
        out.writeVInt(type.ordinal());
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
            type == signature.type &&
            argumentTypes.equals(signature.argumentTypes) &&
            returnType.equals(signature.returnType);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
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
}
