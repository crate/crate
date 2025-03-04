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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.jetbrains.annotations.Nullable;

import io.crate.types.DataType;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;

public class BoundVariables {

    private final Map<String, DataType<?>> typeVariables;
    private final IntObjectMap<DataType<?>> boundTypes;

    public BoundVariables(Map<String, DataType<?>> typeVariables) {
        this(typeVariables, new IntObjectHashMap<>(0));
    }

    public BoundVariables(Map<String, DataType<?>> typeVariables,
                          IntObjectMap<DataType<?>> boundTypes) {
        this.typeVariables = Map.copyOf(typeVariables);
        this.boundTypes = boundTypes;
    }

    public DataType<?> getTypeVariable(String variableName) {
        return typeVariables.get(variableName);
    }

    public boolean containsTypeVariable(String variableName) {
        return typeVariables.containsKey(variableName);
    }

    public Set<String> getTypeVariableNames() {
        return typeVariables.keySet();
    }

    @Nullable
    public DataType<?> getBoundType(int idx) {
        return boundTypes.get(idx);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BoundVariables that = (BoundVariables) o;
        return Objects.equals(typeVariables, that.typeVariables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeVariables);
    }

    @Override
    public String toString() {
        return "BoundVariables{" + "typeVariables=" + typeVariables + '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final Map<String, DataType<?>> typeVariables = new HashMap<>();
        private final IntObjectMap<DataType<?>> boundTypes = new IntObjectHashMap<>();

        public DataType<?> getTypeVariable(String variableName) {
            return typeVariables.get(variableName);
        }

        public void setTypeVariable(String variableName, DataType<?> variableValue) {
            typeVariables.put(variableName, variableValue);
        }

        public boolean containsTypeVariable(String variableName) {
            return typeVariables.containsKey(variableName);
        }

        public boolean setBoundType(int idx, DataType<?> boundType) {
            DataType<?> existingType = boundTypes.put(idx, boundType);
            return !boundType.equals(existingType);
        }

        public BoundVariables build() {
            return new BoundVariables(typeVariables, boundTypes);
        }
    }
}
