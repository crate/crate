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

package io.crate.operation.scalar.elasticsearch.script;

import io.crate.analyze.symbol.Literal;
import io.crate.metadata.Functions;
import io.crate.operation.Input;
import io.crate.operation.operator.Operator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptModule;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class NumericScalarSearchScript extends NumericScalarSortScript {

    public static final String NAME = "numeric_scalar_search";

    public static void register(ScriptModule module) {
        module.registerScript(NAME, Factory.class);
    }

    public static class Factory extends AbstractScalarSearchScriptFactory {

        @Inject
        public Factory(Functions functions) {
            super(functions);
        }

        /**
         * This method is called for every search on every shard.
         *
         * @param params list of script parameters passed with the query
         * @return new native script
         */
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            SearchContext ctx = (SearchContext) prepare(params);
            return new NumericScalarSearchScript(
                    ctx.operator,
                    ctx.function,
                    ctx.operatorArgs);
        }
    }

    private final Operator operator;
    private final List<AbstractScalarScriptFactory.WrappedArgument> operatorArgs;

    public NumericScalarSearchScript(Operator operator,
                                     AbstractScalarScriptFactory.ScalarArgument function,
                                     List<AbstractScalarScriptFactory.WrappedArgument> operatorArgs) {
        super(function, null);
        this.operator = operator;
        this.operatorArgs = operatorArgs;
    }

    @Override
    public Object doRun() {
        Input[] operatorArgInputs = new Input[operatorArgs.size()];
        for (int i = 0; i < operatorArgs.size(); i++) {
            AbstractScalarScriptFactory.WrappedArgument operatorArg = operatorArgs.get(i);
            operatorArgInputs[i] = Literal.newLiteral(
                    operatorArg.getType(),
                    operatorArg.getType().value(operatorArg.evaluate(doc())));
        }
        return operator.evaluate(operatorArgInputs);
    }

}
