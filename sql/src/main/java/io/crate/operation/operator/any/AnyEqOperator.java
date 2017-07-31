/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.operator.any;

import io.crate.analyze.symbol.Function;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.operator.OperatorModule;
import io.crate.sql.tree.ComparisonExpression;

public class AnyEqOperator extends AnyOperator {

    public static final String NAME = OPERATOR_PREFIX + ComparisonExpression.Type.EQUAL.getValue();

    private static class AnyEqResolver extends AnyResolver {

        @Override
        public FunctionImplementation newInstance(FunctionInfo info) {
            return new AnyEqOperator(info);
        }

        @Override
        public String name() {
            return NAME;
        }
    }

    public static void register(OperatorModule module) {
        module.registerDynamicOperatorFunction(NAME, new AnyEqResolver());
    }

    protected AnyEqOperator(FunctionInfo functionInfo) {
        super(functionInfo);
    }

    @Override
    protected boolean compare(int comparisonResult) {
        return comparisonResult == 0;
    }

    @Override
    public String operator(Function function) {
        return "= ANY";
    }
}
