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

package io.crate.operation.scalar.arithmetic;

import io.crate.data.Input;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionResolver;
import io.crate.metadata.Scalar;
import io.crate.metadata.Signature;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * _map(k, v, [...]) -> object
 *
 * args must be a multiple of 2
 *
 * k: must be a string
 *
 * Note that keys will be returned as String while values of type String will be BytesRef
 */
public class MapFunction extends Scalar<Object, Object> {

    public static final String NAME = "_map";

    private static final FunctionResolver RESOLVER = new BaseFunctionResolver(
        // emtpy args or (string, any) pairs
        Signature.ofIterable(() -> new Iterator<Signature.ArgMatcher>() {
            private int pos = 0;

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Signature.ArgMatcher next() {
                return pos++ % 2 == 0 ? Signature.ArgMatcher.STRING : Signature.ArgMatcher.ANY;
            }
        }).preCondition(dt -> dt.size() % 2 == 0)) {

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            return new MapFunction(createInfo(dataTypes));
        }
    };

    private final FunctionInfo info;

    private MapFunction(FunctionInfo info) {
        this.info = info;
    }

    public static FunctionInfo createInfo(List<DataType> dataTypes) {
        return new FunctionInfo(new FunctionIdent(NAME, dataTypes), DataTypes.OBJECT);
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @SafeVarargs
    @Override
    public final Object evaluate(Input<Object>... args) {
        Map<String, Object> m = new HashMap<>(args.length / 2, 1.0f);
        for (int i = 0; i < args.length; i += 2) {
            m.put(BytesRefs.toBytesRef(args[i].value()).utf8ToString(), args[i + 1].value());
        }
        return m;
    }

    public static void register(ScalarFunctionModule scalarFunctionModule) {
        scalarFunctionModule.register(NAME, RESOLVER);
    }
}
