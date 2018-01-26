/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.expression.scalar;

import com.google.common.base.Preconditions;
import io.crate.data.Input;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;

import java.util.List;
import java.util.Locale;

import static io.crate.metadata.functions.params.Param.ANY;
import static io.crate.metadata.functions.params.Param.STRING;

public class FormatFunction extends Scalar<BytesRef, Object> {

    public static final String NAME = "format";
    private FunctionInfo info;

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    private FormatFunction(FunctionInfo info) {
        this.info = info;
    }

    @SafeVarargs
    @Override
    public final BytesRef evaluate(Input<Object>... args) {
        assert args.length > 1 : "number of args must be > 1";
        Object arg0Value = args[0].value();
        assert arg0Value != null : "1st argument must not be null";

        Object[] values = new Object[args.length - 1];
        for (int i = 0; i < args.length - 1; i++) {
            Object value = args[i + 1].value();
            if (value instanceof BytesRef) {
                values[i] = ((BytesRef) value).utf8ToString();
            } else {
                values[i] = value;
            }
        }

        return new BytesRef(String.format(Locale.ENGLISH, ((BytesRef) arg0Value).utf8ToString(), values));
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    private static class Resolver extends BaseFunctionResolver {

        protected Resolver() {
            super(FuncParams.builder(STRING)
                .withIndependentVarArgs(ANY)
                .build());
        }

        private static FunctionInfo createInfo(List<DataType> types) {
            return new FunctionInfo(new FunctionIdent(NAME, types), DataTypes.STRING);
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            Preconditions.checkArgument(dataTypes.size() > 1 && dataTypes.get(0) == DataTypes.STRING);
            return new FormatFunction(createInfo(dataTypes));
        }
    }
}
