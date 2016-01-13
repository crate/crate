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

package io.crate.operation.scalar.string;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.analysis.util.CharacterUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.lucene.BytesRefs;

public class UpperFunction extends Scalar<BytesRef, Object> {
    public final static String NAME = "upper";
    private final static CharacterUtils charUtils = CharacterUtils.getInstance();

    private final FunctionInfo info;

    public static void register(ScalarFunctionModule module) {
        module.register(new UpperFunction(new FunctionInfo(
                new FunctionIdent(NAME, ImmutableList.<DataType>of(DataTypes.STRING)),
                DataTypes.STRING)
        ));
    }

    public UpperFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public BytesRef evaluate(Input<Object>... args) {
        Object stringValue = args[0].value();
        if (stringValue == null) {
            return null;
        }

        BytesRef inputByteRef = BytesRefs.toBytesRef(stringValue);

        char[] ref = new char[inputByteRef.length];
        int len = UnicodeUtil.UTF8toUTF16(inputByteRef.bytes, inputByteRef.offset, inputByteRef.length, ref);
        charUtils.toUpperCase(ref, 0, len);

        byte[] res = new byte[UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR * len];
        len = UnicodeUtil.UTF16toUTF8(ref, 0, len, res);
        return new BytesRef(res, 0, len);
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

}
