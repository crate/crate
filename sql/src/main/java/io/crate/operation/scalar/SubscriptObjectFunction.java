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

import io.crate.metadata.*;
import io.crate.data.Input;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public class SubscriptObjectFunction extends Scalar<Object, Map> {

    public static final String NAME = "subscript_obj";
    private FunctionInfo info;

    public static void register(ScalarFunctionModule module) {
        module.register(NAME,
            new BaseFunctionResolver(Signature.of(Signature.ArgMatcher.OBJECT, Signature.ArgMatcher.STRING)) {
                @Override
                public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
                    return new SubscriptObjectFunction(
                        new FunctionInfo(new FunctionIdent(NAME, dataTypes.subList(0, 2)), DataTypes.UNDEFINED));
                }
            }
        );
    }

    private SubscriptObjectFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Object evaluate(Input[] args) {
        assert args.length == 2 : "invalid number of arguments";
        return evaluate(args[0].value(), args[1].value());
    }

    private Object evaluate(Object element, Object key) {
        if (element == null || key == null) {
            return null;
        }
        assert element instanceof Map : "first argument must be of type Map";
        assert key instanceof BytesRef : "second argument must be of type BytesRef";

        Map m = (Map) element;
        String k = BytesRefs.toString(key);
        if (!m.containsKey(k)) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "The object does not contain [%s] key", k));
        }
        return m.get(k);
    }
}
