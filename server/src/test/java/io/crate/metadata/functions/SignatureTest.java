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

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import io.crate.metadata.FunctionType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.TypeSignature;

public class SignatureTest {

    @Test
    public void test_streaming_of_signature_and_type_signatures() throws Exception {
        var objectType = ObjectType.builder()
            .setInnerType("x", DataTypes.INTEGER)
            .build();

        var signature = Signature.builder()
            .name("foo")
            .kind(FunctionType.SCALAR)
            .argumentTypes(
                TypeSignature.parse("E"),
                DataTypes.INTEGER.getTypeSignature(),
                objectType.getTypeSignature()
            )
            .returnType(DataTypes.BIGINT_ARRAY.getTypeSignature())
            .variableArityGroup(
                List.of(
                    DataTypes.INTEGER.getTypeSignature(),
                    objectType.getTypeSignature()
                )
            )
            .typeVariableConstraints(typeVariable("E"))
            .build();

        BytesStreamOutput out = new BytesStreamOutput();
        signature.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        var signature2 = new Signature(in);

        assertThat(signature2, equalTo(signature));
    }
}
