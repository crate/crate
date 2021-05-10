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

package io.crate.types;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public final class ParameterTypeSignature extends TypeSignature {

    private final String parameterName;

    public ParameterTypeSignature(String parameterName,
                                  TypeSignature typeSignature) {
        super(typeSignature.getBaseTypeName(), typeSignature.getParameters());
        this.parameterName = parameterName;
    }

    public ParameterTypeSignature(StreamInput in) throws IOException {
        super(in);
        parameterName = in.readString();
    }

    public String parameterName() {
        return parameterName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(parameterName);
    }

    @Override
    public TypeSignatureType type() {
        return TypeSignatureType.PARAMETER_TYPE_SIGNATURE;
    }

    @Override
    public String toString() {
        return parameterName + " " + super.toString();
    }
}
