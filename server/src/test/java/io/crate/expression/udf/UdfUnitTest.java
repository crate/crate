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

package io.crate.expression.udf;

import static io.crate.testing.TestingHelpers.createNodeContext;

import javax.script.ScriptException;

import org.jetbrains.annotations.Nullable;
import org.junit.Before;

import io.crate.data.Input;
import io.crate.metadata.NodeContext;
import io.crate.expression.scalar.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public abstract class UdfUnitTest extends CrateDummyClusterServiceUnitTest {

    UserDefinedFunctionService udfService;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        NodeContext nodeContext = createNodeContext();
        udfService = new UserDefinedFunctionService(clusterService, new DocTableInfoFactory(nodeContext), nodeContext);
    }

    public static final UDFLanguage DUMMY_LANG = new UDFLanguage() {
        @Override
        public Scalar createFunctionImplementation(UserDefinedFunctionMetadata metadata,
                                                   Signature signature,
                                                   BoundSignature boundSignature) throws ScriptException {
            return new DummyFunction(signature);
        }

        @Nullable
        @Override
        public String validate(UserDefinedFunctionMetadata metadata) {
            return null;
        }

        @Override
        public String name() {
            return "dummy";
        }
    };

    static class DummyFunction extends Scalar<Integer, Integer> {

        public static final Integer RESULT = -42;


        DummyFunction(Signature signature) {
            super(signature, BoundSignature.sameAsUnbound(signature));
        }

        @Override
        public Integer evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Integer>[] args) {
            return RESULT;
        }
    }
}
