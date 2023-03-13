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

package io.crate.expression;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public class AbstractFunctionModuleTest extends ESTestCase {

    AbstractFunctionModule<FunctionImplementation> module = new AbstractFunctionModule<>() {
        @Override
        public void configureFunctions() {
        }
    };

    private static class DummyFunction implements FunctionImplementation {

        private final Signature signature;

        public DummyFunction(Signature signature) {
            this.signature = signature;
        }

        @Override
        public Signature signature() {
            return signature;
        }

        @Override
        public BoundSignature boundSignature() {
            return BoundSignature.sameAsUnbound(signature);
        }
    }

    @Test
    public void test_registering_function_with_same_signature_raises_an_error() {
        var signature = Signature.scalar(
            "foo",
            DataTypes.INTEGER.getTypeSignature(),
            DataTypes.INTEGER.getTypeSignature()
        );

        module.register(signature, (s, args) -> new DummyFunction(s));

        assertThatThrownBy(() -> module.register(signature, (s, args) -> new DummyFunction(s)))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageStartingWith("A function already exists for signature");
    }


    @Test
    public void test_registering_function_with_signature_only_differ_in_binding_info_raises_an_error() {
        var signature = Signature.scalar(
            "foo",
            DataTypes.INTEGER.getTypeSignature(),
            DataTypes.INTEGER.getTypeSignature()
        );
        module.register(signature, (s, args) -> new DummyFunction(s));

        var signature2 = Signature.scalar(
            "foo",
            DataTypes.INTEGER.getTypeSignature(),
            DataTypes.INTEGER.getTypeSignature()
        ).withVariableArity();

        assertThatThrownBy(() -> module.register(signature2, (s, args) -> new DummyFunction(s)))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageStartingWith("A function already exists for signature");
    }
}
