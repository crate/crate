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

import io.crate.metadata.Functions;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptModule;

import javax.annotation.Nullable;
import java.util.Map;

public class NumericScalarSortScript extends AbstractCatchingSearchScript {

    public static final String NAME = "numeric_scalar_sort";

    public static void register(ScriptModule module) {
        module.registerScript(NAME, Factory.class);
    }

    public static class Factory extends AbstractScalarScriptFactory {

        @Inject
        public Factory(Functions functions) {
            super(functions);
        }

        protected Context prepare(@Nullable Map<String, Object> params) {
            if (params == null) {
                throw new ScriptException("Parameter required");
            }
            Object scalar = params.get("scalar");
            if (scalar == null || !(scalar instanceof Map)) {
                throw new ScriptException("invalid sort scalar format");
            }
            WrappedArgument arg = getArgument((Map<String, Object>)scalar);
            // first argument should be the scalar
            if (!(arg instanceof ScalarArgument)) {
                throw new ScriptException("no scalar used in sort script!");
            }
            return new Context((ScalarArgument) arg);
        }

        /**
         * This method is called for every search on every shard.
         *
         * @param params list of script parameters passed with the query
         * @return new native script
         */
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            Context ctx = prepare(params);

            String missing = XContentMapValues.nodeStringValue(params.get("missing"), null);

            return new NumericScalarSortScript(ctx.function, missing);
        }
    }

    private final AbstractScalarScriptFactory.ScalarArgument function;
    @Nullable
    private final String missing;

    public NumericScalarSortScript(AbstractScalarScriptFactory.ScalarArgument function,
                                   String missing) {
        this.function = function;
        this.missing = missing;
    }

    @Override
    public Object doRun() {
        Object returnValue = this.function.evaluate(doc());

        if (returnValue == null) {
            if (missing != null && missing.equals("_first")) {
                return Double.MIN_VALUE;
            } else {
                return Double.MAX_VALUE;
            }
        } else {
            return returnValue;
        }
    }
}
