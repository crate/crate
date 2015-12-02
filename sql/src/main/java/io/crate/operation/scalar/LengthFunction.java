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

package io.crate.operation.scalar;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;

import java.util.List;

public abstract class LengthFunction extends Scalar<Integer, BytesRef> {

    private static final List<DataType> DATA_TYPES = ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.UNDEFINED);

    private final FunctionInfo functionInfo;

    private LengthFunction(FunctionInfo functionInfo) {
        this.functionInfo = functionInfo;
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    public static void register(ScalarFunctionModule module) {
        for (DataType type : DATA_TYPES) {
            module.register(new OctetLengthFunction(createInfo(OctetLengthFunction.NAME, type)));
            module.register(new BitLengthFunction(createInfo(BitLengthFunction.NAME, type)));
            module.register(new CharLengthFunction(createInfo(CharLengthFunction.NAME, type)));
        }
    }

    protected static FunctionInfo createInfo(String functionName, DataType dataType) {
        return new FunctionInfo(
                new FunctionIdent(functionName, ImmutableList.of(dataType)), DataTypes.INTEGER
        );
    }

    protected BytesRef evaluateInput(Input[] args) {
        assert args.length == 1;
        Object string = args[0].value();
        if (string == null) {
            return null;
        }
        return (BytesRef) string;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        assert symbol.arguments().size() == 1;
        Symbol argument = symbol.arguments().get(0);
        if (argument.symbolType().isValueSymbol()) {
            return Literal.newLiteral(info().returnType(), evaluate((Input) argument));
        }
        return symbol;
    }

    static class OctetLengthFunction extends LengthFunction {
        public static final String NAME = "octet_length";

        protected OctetLengthFunction(FunctionInfo info) {
            super(info);
        }

        public Integer evaluate(Input[] args) {
            BytesRef string = evaluateInput(args);
            return string != null ? string.length : null;
        }
    }

    static class BitLengthFunction extends LengthFunction {
        public static final String NAME = "bit_length";

        protected BitLengthFunction(FunctionInfo info) {
            super(info);
        }

        @Override
        public Integer evaluate(Input[] args) {
            BytesRef string = evaluateInput(args);
            return string != null ? string.length * Byte.SIZE : null;
        }
    }

    static class CharLengthFunction extends LengthFunction {
        public static final String NAME = "char_length";

        protected CharLengthFunction(FunctionInfo info) {
            super(info);
        }

        @Override
        public Integer evaluate(Input[] args) {
            BytesRef string = evaluateInput(args);
            return string != null ? string.utf8ToString().length() : null;
        }
    }

}
