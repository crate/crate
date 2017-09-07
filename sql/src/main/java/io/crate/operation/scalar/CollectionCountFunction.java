/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.data.Input;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionResolver;
import io.crate.metadata.Scalar;
import io.crate.metadata.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Collection;
import java.util.List;

public class CollectionCountFunction extends Scalar<Long, Collection<DataType>> {

    public static final String NAME = "collection_count";
    private final FunctionInfo info;

    private static final FunctionResolver collectionCountResolver = new CollectionCountResolver();

    public static void register(ScalarFunctionModule mod) {
        mod.register(NAME, collectionCountResolver);
    }

    CollectionCountFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public Long evaluate(Input<Collection<DataType>>... args) {
        // TODO: eliminate Integer.MAX_VALUE limitation of Set.size()
        Collection<DataType> arg0Value = args[0].value();
        if (arg0Value == null) {
            return null;
        }
        return ((Integer) (arg0Value.size())).longValue();
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    static class CollectionCountResolver extends BaseFunctionResolver {

        CollectionCountResolver() {
            super(Signature.of(Signature.ArgMatcher.ANY_SET));
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            return new CollectionCountFunction(new FunctionInfo(
                new FunctionIdent(NAME, dataTypes),
                DataTypes.LONG
            ));
        }
    }
}
