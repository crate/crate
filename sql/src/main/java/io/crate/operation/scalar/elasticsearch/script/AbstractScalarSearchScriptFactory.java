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
import io.crate.planner.symbol.Literal;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.script.ScriptException;

import javax.annotation.Nullable;
import java.util.Map;

public abstract class AbstractScalarSearchScriptFactory extends AbstractScalarScriptFactory {

    protected Operator operator;
    protected Literal valueLiteral;

    protected AbstractScalarSearchScriptFactory(Functions functions) {
        super(functions);
    }

    @Override
    protected void prepare(@Nullable Map<String, Object> params) {
        super.prepare(params);

        String operatorName = XContentMapValues.nodeStringValue(params.get("op"), null);
        if (operatorName == null) {
            throw new ScriptException("Missing the op parameter");
        }

        if (params.containsKey("value")) {
            Number value = XContentMapValues.nodeDoubleValue(params.get("value"));
            valueLiteral = Literal.newLiteral(value.doubleValue());
        } else {
            throw new ScriptException("Missing the value parameter");
        }

        FunctionIdent operatorIdent = new FunctionIdent(operatorName, ImmutableList.of(fieldType, fieldType));
        operator = (Operator)functions.get(operatorIdent);
        if (operator == null) {
            throw new ScriptException(String.format("Cannot resolve operator with ident %s", operatorIdent));
        }
    }
}
