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
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.planner.symbol.Literal;
import io.crate.types.*;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractScalarScriptFactory implements NativeScriptFactory {

    protected final Functions functions;

    protected String fieldName;
    protected DataType fieldType;
    protected Scalar function;
    protected List<Input> arguments;

    public AbstractScalarScriptFactory(Functions functions) {
        this.functions = functions;
    }

    protected void prepare(@Nullable Map<String, Object> params) {
        if (params == null) {
            throw new ScriptException("Parameter required");
        }

        fieldName = XContentMapValues.nodeStringValue(params.get("field_name"), null);
        if (fieldName == null) {
            throw new ScriptException("Missing the field_name parameter");
        }

        String fieldTypeName = XContentMapValues.nodeStringValue(params.get("field_type"), null);
        if (fieldTypeName == null) {
            throw new ScriptException("Missing the field_type parameter");
        }
        fieldType = DataTypes.ofName(fieldTypeName);
        if (fieldType.id() >= ShortType.ID && fieldType.id() <= TimestampType.ID) {
            fieldType = LongType.INSTANCE;
        } else if (fieldType.id() >= DoubleType.ID && fieldType.id() <= FloatType.ID) {
            fieldType = DoubleType.INSTANCE;
        }

        List<DataType> argumentTypes = null;
        if (params.containsKey("args") && XContentMapValues.isArray(params.get("args"))) {
            List args = (List)params.get("args");
            arguments = new ArrayList<>(args.size());
            argumentTypes = new ArrayList<>(args.size());
            argumentTypes.add(fieldType);
            for (Object arg : args) {
                arguments.add(Literal.newLiteral(XContentMapValues.nodeDoubleValue(arg)));
                argumentTypes.add(DoubleType.INSTANCE);
            }
        }

        FunctionIdent functionIdent;
        if (argumentTypes == null) {
            functionIdent = new FunctionIdent(functionName(), ImmutableList.of(fieldType));
        } else {
            functionIdent = new FunctionIdent(functionName(), argumentTypes);
        }
        function = (Scalar)functions.get(functionIdent);
        if (function == null) {
            throw new ScriptException(String.format("Cannot resolve function with ident %s", functionIdent));
        }
    }

    protected abstract String functionName();
}
