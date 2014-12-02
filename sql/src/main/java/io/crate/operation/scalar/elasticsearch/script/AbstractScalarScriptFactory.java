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
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.search.lookup.DocLookup;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;


/**
 * expects the following parameters:
 *
 * {
 *     "op": "operatorName",
 *     "args": [argument, ...]
 * }
 * <p>
 *
 * where argument can be one of:
 * <p>
 *
 * {
 *     "value": "bla",
 *     "type": 1
 * }
 * <p>
 * {
 *     "scalar_name": "my_scalar",
 *     "args": [argument, ...],
 *     "type": 1
 * }
 * <p>
 * {
 *     "field_name": "name",
 *     "type": 3
 * }
 *
 */
public abstract class AbstractScalarScriptFactory implements NativeScriptFactory {

    protected static class Context {
        public final ScalarArgument function;

        public Context(ScalarArgument function) {
            this.function = function;
        }
    }

    abstract static class WrappedArgument {
        public abstract DataType getType();

        public abstract Object evaluate(DocLookup doc);
    }

    /**
     * wrapper for literals or references (name and type)
     */
    public static class LiteralArgument extends WrappedArgument {
        public final Literal value;

        LiteralArgument(Literal value) {
            this.value = value;
        }

        @Override
        public DataType getType() {
            return value.valueType();
        }

        @Override
        public Object evaluate(DocLookup doc) {
            return value.value();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof LiteralArgument)) return false;

            LiteralArgument that = (LiteralArgument) o;

            if (!value.equals(that.value)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }

    /**
     * wrapper for references
     */
    public static class ReferenceArgument extends WrappedArgument {
        public final String fieldName;
        public final DataType type;

        ReferenceArgument(String fieldName, DataType type) {
            this.fieldName = fieldName;
            this.type = type;
        }

        @Override
        public DataType getType() {
            return type;
        }

        @Override
        public Object evaluate(DocLookup doc) {
            ScriptDocValues docValues = (ScriptDocValues)doc.get(fieldName);
            if (docValues == null || docValues.isEmpty()) {
                return null;
            } else {
                if (type.equals(DataTypes.DOUBLE)) {
                    return ((ScriptDocValues.Doubles)docValues).getValue();
                } else if (type.equals(DataTypes.LONG)) {
                    return ((ScriptDocValues.Longs)docValues).getValue();
                } else {
                    throw new ScriptException(
                            String.format("Field data type not supported"));
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ReferenceArgument)) return false;

            ReferenceArgument that = (ReferenceArgument) o;

            if (!fieldName.equals(that.fieldName)) return false;
            if (!type.equals(that.type)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = fieldName.hashCode();
            result = 31 * result + type.hashCode();
            return result;
        }
    }


    /**
     * wrapper for scalar function with included arguments
     */
    public static class ScalarArgument extends WrappedArgument {
        public final Scalar function;
        public final DataType type;
        public final List<WrappedArgument> args;

        ScalarArgument(Scalar function, DataType returnType, List<WrappedArgument> args) {
            this.function = function;
            this.type = returnType;
            this.args = args;
        }

        @Override
        public DataType getType() {
            return type;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object evaluate(DocLookup doc) {
            Input[] argumentInputs = new Input[args.size()];
            for (int i = 0; i < args.size(); i++) {
                WrappedArgument arg = args.get(i);
                Object argValue = arg.evaluate(doc);
                argumentInputs[i] = Literal.newLiteral(arg.getType(), argValue);
            }
            return function.evaluate(argumentInputs);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ScalarArgument)) return false;

            ScalarArgument that = (ScalarArgument) o;

            if (!args.equals(that.args)) return false;
            if (!function.equals(that.function)) return false;
            if (!type.equals(that.type)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = function.hashCode();
            result = 31 * result + type.hashCode();
            result = 31 * result + args.hashCode();
            return result;
        }
    }

    protected final Functions functions;

    public AbstractScalarScriptFactory(Functions functions) {
        this.functions = functions;
    }

    /**
     * extract datatype, converts types to LONG for integer numeric types, to DOUBLE for decimal numeric types
     *
     * @param params
     * @param name
     * @return
     */
    private static DataType extractDataType(Map<String, Object> params, String name) {
        Object type = params.get(name);
        if (type == null) {
            throw new ScriptException(String.format(Locale.ENGLISH, "Could not extract %s parameter", name));
        }
        DataType dataType = DataTypes.ofJsonObject(type);
        if (dataType.id() == ByteType.ID || (dataType.id() >= ShortType.ID && dataType.id() <= TimestampType.ID)) {
            dataType = LongType.INSTANCE;
        } else if (dataType.id() >= DoubleType.ID && dataType.id() <= FloatType.ID) {
            dataType = DoubleType.INSTANCE;
        }
        return dataType;
    }

    protected WrappedArgument getArgument(Map<String, Object> argSpec) {
        // TODO: create and use a Map<String, Object> visitor
        DataType type = extractDataType(argSpec, "type");
        if (argSpec.containsKey("value") && argSpec.containsKey("type")) {
            return new LiteralArgument(Literal.newLiteral(type, type.value(argSpec.get("value"))));
        } else if (argSpec.containsKey("scalar_name")) {

            String scalarName = XContentMapValues.nodeStringValue(argSpec.get("scalar_name"), null);
            if (scalarName == null) {
                throw new ScriptException(String.format("No scalar_name given"));
            }
            List<WrappedArgument> wrappedArgs = ImmutableList.of();
            List<DataType> argumentTypes = ImmutableList.of();
            if (argSpec.containsKey("args") && XContentMapValues.isArray(argSpec.get("args"))) {
                List args = (List)argSpec.get("args");
                argumentTypes = new ArrayList<>(args.size());
                wrappedArgs = new ArrayList<>(args.size());
                for (Object arg : args) {
                    assert arg instanceof Map;
                    WrappedArgument argument = getArgument((Map<String, Object>)arg);
                    argumentTypes.add(argument.getType());
                    wrappedArgs.add(argument);
                }
            }
            Scalar scalar = getScalar(scalarName, argumentTypes);
            if (scalar == null) {
                throw new ScriptException(String.format("Cannot resolve function %s", scalarName));
            }
            return new ScalarArgument(scalar, type, wrappedArgs);

        } else if (argSpec.containsKey("field_name")) {
            String fieldName = XContentMapValues.nodeStringValue(argSpec.get("field_name"), null);
            if (fieldName == null) {
                throw new ScriptException("Missing the field_name parameter");
            }
            return new ReferenceArgument(fieldName, type);
        } else {
            throw new ScriptException("invalid parameter format");
        }
    }

    protected @Nullable Scalar getScalar(String name, @Nullable List<DataType> argumentTypes) {
        FunctionIdent functionIdent;
        if (argumentTypes == null|| argumentTypes.isEmpty()) {
            functionIdent = new FunctionIdent(name, ImmutableList.<DataType>of());
        } else {
            functionIdent = new FunctionIdent(name, argumentTypes);
        }
        return (Scalar)functions.get(functionIdent);
    }
}
