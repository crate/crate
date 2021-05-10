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

package io.crate.metadata;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import io.crate.common.collections.EnumSets;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

/**
 * @deprecated Use {@link Signature} instead. Exists only for BWC and will be removed with the next major version
 */
public final class FunctionInfo implements Writeable {

    private final FunctionIdent ident;
    private final DataType<?> returnType;
    private final FunctionType type;
    private final Set<Scalar.Feature> features;

    /**
     * Create FunctionInfo based on a bounded signature.
     * (If a signature is bounded, possible type variable constraints are replaced with concrete data types)
     */
    public static FunctionInfo of(Signature signature) {
        return new FunctionInfo(
            new FunctionIdent(signature.getName(), signature.getArgumentDataTypes()),
            signature.getReturnType().createType(),
            signature.getKind(),
            signature.getFeatures()
        );
    }

    /**
     * Create FunctionInfo based on a declared signature which may contain type variable constraints.
     * Thus concrete argument types and return type arguments are needed.
     */
    public static FunctionInfo of(Signature signature, List<DataType<?>> argumentTypes, DataType<?> returnType) {
        return new FunctionInfo(
            new FunctionIdent(signature.getName(), argumentTypes),
            returnType,
            signature.getKind(),
            signature.getFeatures()
        );
    }

    public FunctionInfo(FunctionIdent ident, DataType<?> returnType, FunctionType type, Set<Scalar.Feature> features) {
        assert features.size() < 32 : "features size must not exceed 32";
        this.ident = ident;
        this.returnType = returnType;
        this.type = type;
        this.features = features;
    }

    public FunctionIdent ident() {
        return ident;
    }

    public FunctionType type() {
        return type;
    }

    public DataType<?> returnType() {
        return returnType;
    }

    public Set<Scalar.Feature> features() {
        return features;
    }

    public boolean hasFeature(Scalar.Feature feature) {
        return features.contains(feature);
    }

    public boolean isDeterministic() {
        return hasFeature(Scalar.Feature.DETERMINISTIC);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FunctionInfo info = (FunctionInfo) o;
        return Objects.equals(ident, info.ident) &&
               Objects.equals(returnType, info.returnType) &&
               type == info.type &&
               Objects.equals(features, info.features);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ident, returnType, type, features);
    }

    @Override
    public String toString() {
        return "FunctionInfo{" +
               "type=" + type +
               ", ident=" + ident +
               ", returnType=" + returnType +
               ", features=" + features +
               '}';
    }

    public FunctionInfo(StreamInput in) throws IOException {
        ident = new FunctionIdent(in);

        returnType = DataTypes.fromStream(in);
        type = FunctionType.values()[in.readVInt()];

        int enumElements = in.readVInt();
        this.features = Collections.unmodifiableSet(EnumSets.unpackFromInt(enumElements, Scalar.Feature.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ident.writeTo(out);
        DataTypes.toStream(returnType, out);
        out.writeVInt(type.ordinal());
        out.writeVInt(EnumSets.packToInt(features));
    }

}
