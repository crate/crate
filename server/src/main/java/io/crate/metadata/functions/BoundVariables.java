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

import io.crate.types.DataType;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class BoundVariables {

    private final Map<String, DataType<?>> typeVariables;

    public BoundVariables(Map<String, DataType<?>> typeVariables) {
        this.typeVariables = Map.copyOf(typeVariables);
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

        public DataType<?> getTypeVariable(String variableName) {
            return typeVariables.get(variableName);
        }

        public void setTypeVariable(String variableName, DataType<?> variableValue) {
            typeVariables.put(variableName, variableValue);
        }

        public boolean containsTypeVariable(String variableName) {
            return typeVariables.containsKey(variableName);
        }

        public BoundVariables build() {
            return new BoundVariables(typeVariables);
        }
    }
}
