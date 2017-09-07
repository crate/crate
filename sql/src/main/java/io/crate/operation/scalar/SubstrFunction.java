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

import com.google.common.annotations.VisibleForTesting;
import io.crate.data.Input;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

import javax.annotation.Nonnull;
import java.util.List;

public class SubstrFunction extends Scalar<BytesRef, Object> {

    public static final String NAME = "substr";
    private static final BytesRef EMPTY_BYTES_REF = new BytesRef("");

    private FunctionInfo info;

    private SubstrFunction(FunctionInfo info) {
        this.info = info;
    }

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public BytesRef evaluate(Input[] args) {
        assert args.length == 2 || args.length == 3 : "number of arguments must be 2 or 3";
        final Object val = args[0].value();
        if (val == null) {
            return null;
        }
        Number beginIdx = (Number) args[1].value();
        if (beginIdx == null) {
            return null;
        }
        if (args.length == 3) {
            Number len = (Number) args[2].value();
            if (len == null) {
                return null;
            }
            return evaluate(BytesRefs.toBytesRef(val),
                (beginIdx).intValue(),
                len.intValue());

        }
        return evaluate(BytesRefs.toBytesRef(val), (beginIdx).intValue());
    }

    private static BytesRef evaluate(@Nonnull BytesRef inputStr, int beginIdx) {
        final int startPos = Math.max(0, beginIdx - 1);
        if (startPos > inputStr.length - 1) {
            return EMPTY_BYTES_REF;
        }
        int endPos = inputStr.length;
        return substring(inputStr, startPos, endPos);
    }

    @VisibleForTesting
    static BytesRef evaluate(@Nonnull BytesRef inputStr, int beginIdx, int len) {
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

    @VisibleForTesting
    static BytesRef substring(BytesRef utf8, int begin, int end) {
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
        if (pos > limit) throw new IllegalArgumentException("begin index must not be > end index");
        return new BytesRef(bytes, posBegin, pos - posBegin);
    }

    private static class Resolver extends BaseFunctionResolver {

        protected Resolver() {
            super(Signature.numArgs(2, 3).and(
                Signature.withLenientVarArgs(Signature.ArgMatcher.STRING, Signature.ArgMatcher.NUMERIC)));
        }

        private static FunctionInfo createInfo(List<DataType> types) {
            return new FunctionInfo(new FunctionIdent(NAME, types), DataTypes.STRING);
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            return new SubstrFunction(createInfo(dataTypes));
        }
    }
}
