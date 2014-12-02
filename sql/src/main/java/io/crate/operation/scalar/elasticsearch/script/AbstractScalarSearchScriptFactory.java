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

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.operation.operator.Operator;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.script.ScriptException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public abstract class AbstractScalarSearchScriptFactory extends AbstractScalarScriptFactory {

    protected AbstractScalarSearchScriptFactory(Functions functions) {
        super(functions);
    }

    protected static class SearchContext extends Context {
        public final Operator operator;
        public final ImmutableList<WrappedArgument> operatorArgs;

        protected SearchContext(ScalarArgument function,
                                Operator operator,
                                List<WrappedArgument> operatorArgs) {
            super(function);
            this.operator = operator;
            this.operatorArgs = ImmutableList.copyOf(operatorArgs);
        }
    }

    protected Context prepare(@Nullable Map<String, Object> params) {
        if (params == null) {
            throw new ScriptException("Parameter required");
        }

        String operatorName = XContentMapValues.nodeStringValue(params.get("op"), null);
        if (operatorName == null) {
            throw new ScriptException("Missing the op parameter");
        }

        // extract operator arguments
        if (!params.containsKey("args") || !XContentMapValues.isArray(params.get("args"))) {
            throw new ScriptException(String.format(Locale.ENGLISH, "No parameters given for operator %s", operatorName));
        }
        List args = (List)params.get("args");
        List<WrappedArgument> operatorArgs = new ArrayList<>(args.size());
        List<DataType> operatorArgTypes = new ArrayList<>(args.size());
        for (Object arg : args) {
            assert arg instanceof Map;
            WrappedArgument argument = getArgument((Map<String, Object>)arg);
            operatorArgs.add(argument);
            // TODO: use real types from argument here
            operatorArgTypes.add(DataTypes.DOUBLE);
        }

        // first argument should be the scalar
        if (!(operatorArgs.get(0) instanceof ScalarArgument)) {
            throw new ScriptException("first argument in search script no scalar!");
        }
        ScalarArgument function = ((ScalarArgument) operatorArgs.get(0));

        // resolve operator
        FunctionIdent operatorIdent = new FunctionIdent(operatorName, operatorArgTypes);
        Operator operator = (Operator)functions.get(operatorIdent);
        if (operator == null) {
            throw new ScriptException(String.format("Cannot resolve operator with ident %s", operatorIdent));
        }

        return new SearchContext(function, operator, operatorArgs);
    }
}
