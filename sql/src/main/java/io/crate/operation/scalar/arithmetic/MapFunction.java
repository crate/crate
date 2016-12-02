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

import io.crate.metadata.*;
import io.crate.operation.Input;
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

    private final static String NAME = "_map";

    private final static DynamicFunctionResolver RESOLVER = new DynamicFunctionResolver() {
        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            if (dataTypes.size() % 2 != 0) {
                throw new IllegalArgumentException(
                    "Number of arguments to _map(...) must be a multiple of 2. Got " + dataTypes.size());
            }
            Iterator<DataType> it = dataTypes.iterator();
            while (it.hasNext()) {
                DataType keyType = it.next();
                if (!keyType.equals(DataTypes.STRING)) {
                    throw new IllegalArgumentException("Keys to _map must be of type string. Got " + keyType);
                }
                it.next();
            }
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
