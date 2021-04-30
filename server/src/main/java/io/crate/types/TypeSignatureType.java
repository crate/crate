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
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.List;

public enum TypeSignatureType {

    TYPE_SIGNATURE(TypeSignature::new),
    PARAMETER_TYPE_SIGNATURE(ParameterTypeSignature::new),
    INTEGER_LITERAL_SIGNATURE(IntegerLiteralTypeSignature::new);

    public static final List<TypeSignatureType> VALUES = List.of(values());

    private final Writeable.Reader<TypeSignature> reader;

    TypeSignatureType(Writeable.Reader<TypeSignature> reader) {
        this.reader = reader;
    }

    public TypeSignature newInstance(StreamInput in) throws IOException {
        return reader.read(in);
    }
}
