/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.expression.udf;

import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.cluster.service.ClusterService;

import javax.annotation.Nullable;
import javax.script.ScriptException;

import static io.crate.testing.TestingHelpers.getFunctions;
import static org.mockito.Mockito.mock;

public abstract class UdfUnitTest extends CrateUnitTest {

    UserDefinedFunctionService udfService = new UserDefinedFunctionService(mock(ClusterService.class), getFunctions());

    static final UDFLanguage DUMMY_LANG = new UDFLanguage() {
        @Override
        public Scalar createFunctionImplementation(UserDefinedFunctionMetaData metaData) throws ScriptException {
            FunctionInfo info = new FunctionInfo(
                new FunctionIdent(metaData.schema(), metaData.name(), metaData.argumentTypes()),
                metaData.returnType()
            );
            return new DummyFunction(info);
        }

        @Nullable
        @Override
        public String validate(UserDefinedFunctionMetaData metadata) {
            return null;
        }

        @Override
        public String name() {
            return "dummy";
        }
    };

    static class DummyFunction extends Scalar<Integer, Integer> {

        public static final Integer RESULT = -42;

        private FunctionInfo info;

        DummyFunction(FunctionInfo info) {
            this.info = info;
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        @Override
        public Integer evaluate(Input<Integer>[] args) {
            return RESULT;
        }
    }
}
