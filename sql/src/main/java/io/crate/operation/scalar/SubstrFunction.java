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

package io.crate.operation.scalar;

import com.google.common.base.Preconditions;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

import javax.annotation.Nonnull;
import java.util.List;

public class SubstrFunction extends Scalar<BytesRef, Object> implements DynamicFunctionResolver {

    public static final String NAME = "substr";
    private FunctionInfo info;
    private static final BytesRef EMPTY_BYTES_REF = new BytesRef("");

    public SubstrFunction() {}
    public SubstrFunction(FunctionInfo info) {
        this.info = info;
    }

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new SubstrFunction());
    }

    private static FunctionInfo createInfo(List<DataType> types) {
        return new FunctionInfo(new FunctionIdent(NAME, types), DataTypes.STRING);
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public BytesRef evaluate(Input[] args) {
        assert (args.length >= 2 && args.length <= 3);
        final Object val = args[0].value();
        if (val == null) {
            return null;
        }
        if (args.length == 3) {
            return evaluate(BytesRefs.toBytesRef(val),
                    ((Number) args[1].value()).intValue(),
                    ((Number) args[2].value()).intValue());

        }
        return evaluate(BytesRefs.toBytesRef(val), ((Number) args[1].value()).intValue());
    }

    private static BytesRef evaluate(@Nonnull BytesRef inputStr, int beginIdx) {
        final int startPos = Math.max(0, beginIdx - 1);
        if (startPos > inputStr.length - 1) {
            return EMPTY_BYTES_REF;
        }
        int endPos = inputStr.length;
        return substring(inputStr, startPos, endPos);
    }

    private static BytesRef evaluate(@Nonnull BytesRef inputStr, int beginIdx, int len) {
        final int startPos = Math.max(0, beginIdx - 1);
        if (startPos > inputStr.length - 1) {
            return EMPTY_BYTES_REF;
        }
        int endPos = inputStr.length;
        if (startPos + len < endPos) {
            endPos = startPos + len;
        }
        return substring(inputStr, startPos, endPos);
    }

    public static BytesRef substring(BytesRef utf8, int begin, int end) {
        int pos = utf8.offset;
        final int limit = pos + utf8.length;
        final byte[] bytes = utf8.bytes;
        int posBegin = pos;

        int codePointCount = 0;
        for (; pos < limit; codePointCount++) {
            if (codePointCount == begin) {
                posBegin = pos;
            }
            if (codePointCount == end) {
                break;
            }

            int v = bytes[pos] & 0xFF;
            if (v <   /* 0xxx xxxx */ 0x80) {
                pos += 1;
                continue;
            }
            if (v >=  /* 110x xxxx */ 0xc0) {
                if (v < /* 111x xxxx */ 0xe0) {
                    pos += 2;
                    continue;
                }
                if (v < /* 1111 xxxx */ 0xf0) {
                    pos += 3;
                    continue;
                }
                if (v < /* 1111 1xxx */ 0xf8) {
                    pos += 4;
                    continue;
                }
                // fallthrough, consider 5 and 6 byte sequences invalid.
            }

            // Anything not covered above is invalid UTF8.
            throw new IllegalArgumentException("substr: invalid UTF8 string found.");
        }

        // Check if we didn't go over the limit on the last character.
        if (pos > limit) throw new IllegalArgumentException();
        return new BytesRef(bytes, posBegin, pos - posBegin);
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        final int size = symbol.arguments().size();
        assert (size >= 2 && size <= 3);

        final Symbol input = symbol.arguments().get(0);
        final Symbol beginIdx = symbol.arguments().get(1);

        if (anyNonLiterals(input, beginIdx, symbol.arguments())) {
            return symbol;
        }

        final Object inputValue = ((Input) input).value();
        final Object beginIdxValue = ((Input) beginIdx).value();
        if (inputValue == null || beginIdxValue == null) {
            return Literal.NULL;
        }
        if (size == 3) {
            if (((Input)symbol.arguments().get(2)).value() == null) {
                return Literal.NULL;
            }
            return Literal.newLiteral(
                    evaluate(BytesRefs.toBytesRef(inputValue),
                    ((Number) ((Input) beginIdx).value()).intValue(),
                    ((Number) ((Input) symbol.arguments().get(2)).value()).intValue()));
        }
        return Literal.newLiteral(evaluate(
                BytesRefs.toBytesRef(inputValue),
                ((Number) ((Input) beginIdx).value()).intValue()));
    }

    private static boolean anyNonLiterals(Symbol input, Symbol beginIdx, List<Symbol> arguments) {
        return !input.symbolType().isValueSymbol() ||
                !beginIdx.symbolType().isValueSymbol() ||
                (arguments.size() == 3 && !arguments.get(2).symbolType().isValueSymbol());
    }

    @Override
    public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
        Preconditions.checkArgument(dataTypes.size() > 1 && dataTypes.size() < 4 && dataTypes.get(0) == DataTypes.STRING);
        return new SubstrFunction(createInfo(dataTypes));
    }
}
