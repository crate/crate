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

import io.crate.types.TypeSignature;

import java.util.List;

/**
 * Containing {@link Signature} properties which are only required for signature binding/matching.
 * It won't be streamed and all properties won't be taken into account when resolving a function by signature.
 */
public class SignatureBindingInfo {

    private final List<TypeVariableConstraint> typeVariableConstraints;
    private final List<TypeSignature> variableArityGroup;
    private final boolean variableArity;
    private final boolean allowCoercion;

    public SignatureBindingInfo(List<TypeVariableConstraint> typeVariableConstraints,
                                List<TypeSignature> variableArityGroup,
                                boolean variableArity,
                                boolean allowCoercion) {
        this.typeVariableConstraints = typeVariableConstraints;
        this.variableArityGroup = variableArityGroup;
        this.variableArity = variableArity;
        this.allowCoercion = allowCoercion;
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
}
