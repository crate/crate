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

package io.crate.expression.scalar.string;

import com.google.common.collect.ImmutableList;
import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.types.DataTypes;
import org.apache.lucene.analysis.CharacterUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.function.BiFunction;

public final class StringCaseFunction {

    public static void register(ScalarFunctionModule module) {
        module.register(new StringCase("upper", (ref, len) -> {
            CharacterUtils.toUpperCase(ref, 0, len);
            return ref;
        }));
        module.register(new StringCase("lower", (ref, len) -> {
            CharacterUtils.toLowerCase(ref, 0, len);
            return ref;
        }));
    }

    private static class StringCase extends Scalar<BytesRef, Object> {
        private final FunctionInfo info;
        private final BiFunction<char[], Integer, char[]> func;

        StringCase(String name, BiFunction<char[], Integer, char[]> func) {
            this.info = new FunctionInfo(new FunctionIdent(name, ImmutableList.of(DataTypes.STRING)), DataTypes.STRING);
            this.func = func;
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        @SafeVarargs
        @Override
        public final BytesRef evaluate(Input<Object>... args) {
            Object stringValue = args[0].value();
            if (stringValue == null) {
                return null;
            }

            BytesRef inputByteRef = BytesRefs.toBytesRef(stringValue);
            char[] ref = new char[inputByteRef.length];
            int len = UnicodeUtil.UTF8toUTF16(inputByteRef.bytes, inputByteRef.offset, inputByteRef.length, ref);

            ref = func.apply(ref, len);

            byte[] res = new byte[UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR * len];
            len = UnicodeUtil.UTF16toUTF8(ref, 0, len, res);
            return new BytesRef(res, 0, len);
        }
    }
}
