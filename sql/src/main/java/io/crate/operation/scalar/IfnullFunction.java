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

package io.crate.operation.scalar;

import com.google.common.base.Preconditions;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;

import java.util.List;
import java.util.Locale;

/**
 *
 * IfnullFunction class to implement the ifnull(expr1, expr2) database function
 * (based on and in the style of the ConcatFunction)
 *
 * @author: kanisfluh
 * @version: 0.1
 *
 */

public abstract class IfnullFunction extends Scalar<BytesRef, BytesRef> {

    public static final String NAME = "ifnull";
    private static final BytesRef EMPTY_STRING = new BytesRef("");
    private FunctionInfo functionInfo;

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    protected IfnullFunction(FunctionInfo functionInfo) {
        this.functionInfo = functionInfo;
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }


    @Override
    public Symbol normalizeSymbol(Function function) {
        if (anyNonLiterals(function.arguments())) {
            return function;
        }
        Input[] inputs = new Input[function.arguments().size()];
        for (int i = 0; i < function.arguments().size(); i++) {
            inputs[i] = ((Input) function.arguments().get(i));
        }
        //noinspection unchecked
        return Literal.newLiteral(functionInfo.returnType(), evaluate(inputs));
    }

    /**
     * specific class handling cases where both input parameters of ifnull(expr1, expr2) are strings
     */

    private static class StringIfnullFunction extends IfnullFunction {

        protected StringIfnullFunction(FunctionInfo functionInfo) {
            super(functionInfo);
        }

        @Override
        public BytesRef evaluate(Input[] args) {
            BytesRef firstArg = (BytesRef) args[0].value();
            BytesRef secondArg = (BytesRef) args[1].value();

            // special case: return "(null)" if both arguments are null ...
            if ((firstArg == null) && (secondArg == null))
                return new BytesRef("(null)");  // not the EMPTY_STRING ...

            // in every other case where the first argument is null, simply return the second argument
            if ((firstArg == null) && (secondArg != null)) {
                return secondArg;
            }

            // came until here: first argument is not null, so return the first argument ...
            if (firstArg != null) {
                return firstArg;
            }

            // we should never get here
            return new BytesRef("not implemented.");
        }
    }

    /**
     * 'generic' class handling all other variations of incoming datatypes of ifnull(expr1, expr2)
     */

    private static class GenericIfnullFunction extends IfnullFunction {

        protected GenericIfnullFunction(FunctionInfo functionInfo) {
            super(functionInfo);
        }

        @Override
        public BytesRef evaluate(Input[] args) {
            BytesRef[] bytesRefs = new BytesRef[args.length];
            int numBytes = 0;

            if (args.length > 2)
                throw new IllegalArgumentException("too many arguments given to ifnull function");

            for (int i = 0; i < args.length; i++) {
                Input input = args[i];
                BytesRef value = DataTypes.STRING.value(input.value());

                bytesRefs[i] = value;
            }

            BytesRef firstArg = (BytesRef) bytesRefs[0];
            BytesRef secondArg = (BytesRef) bytesRefs[1];

            // and now the same proceeding as is done in the non-generic StringIfnullFunction ...

            // special case: return "(null)" if both arguments are null ...
            if ((firstArg == null) && (secondArg == null))
                return new BytesRef("(null)");  // not the EMPTY_STRING ...

            // in every other case where the first argument is null, simply return the second argument
            if ((firstArg == null) && (secondArg != null)) {
                //byte[] bytes = new byte[secondArg.length];
                //System.arraycopy(secondArg.bytes, 0, bytes, 0, secondArg.length);
                return secondArg;
            }

            // came until here: first argument is not null, so return the first argument ...
            if (firstArg != null) {
                //byte[] bytes = new byte[firstArg.length];
                //System.arraycopy(firstArg.bytes, 0, bytes, 0, firstArg.length);
                return firstArg;
            }

            // we should never get here
            return new BytesRef("not implemented.");

        }
    }

    private static class Resolver implements DynamicFunctionResolver {

        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            if (dataTypes.size() < 2) {
                throw new IllegalArgumentException("ifnull function requires exactly 2 arguments");
            } else if (dataTypes.size() == 2 &&
                    dataTypes.get(0).equals(DataTypes.STRING) &&
                    dataTypes.get(1).equals(DataTypes.STRING)) {
                return new StringIfnullFunction(new FunctionInfo(new FunctionIdent(NAME, dataTypes), DataTypes.STRING));

            } else {
                for (int i = 0; i < dataTypes.size(); i++) {
                    if (!dataTypes.get(i).isConvertableTo(DataTypes.STRING)) {
                        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                "Argument %d of the ifnull function can't be converted to string", i));
                    }
                }
                return new GenericIfnullFunction(new FunctionInfo(new FunctionIdent(NAME, dataTypes), DataTypes.STRING));
            }
        }
    }

}
