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

import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;

import java.util.Collection;
import java.util.List;

public class CollectionCountFunction implements Scalar<Long, Collection<DataType>> {

    public static final String NAME = "collection_count";
    private final FunctionInfo info;

    private static final DynamicFunctionResolver collectionCountResolver = new CollectionCountResolver();

    public static void register(ScalarFunctionModule mod) {
        mod.register(NAME, collectionCountResolver);
    }

    public CollectionCountFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public Long evaluate(Input<Collection<DataType>>... args) {
        // TODO: eliminate Integer.MAX_VALUE limitation of Set.size()
        if (args[0].value() == null) {
            return null;
        }
        return ((Integer)((args[0].value()).size())).longValue();
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function function) {
        return function;
    }

    static class CollectionCountResolver implements DynamicFunctionResolver {

        private static boolean isCollectionType(DataType dataType) {
            return dataType.id() == ArrayType.ID || dataType.id() == SetType.ID;
        }

        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            for (DataType dataType : dataTypes) {
                if (!isCollectionType(dataType)) {
                    throw new IllegalArgumentException(String.format(
                            "Function \"%s\" got an invalid argument of type \"%s\"",
                            NAME,
                            dataType.getName()));
                }
            }

            return new CollectionCountFunction(new FunctionInfo(
                    new FunctionIdent(NAME, dataTypes),
                    DataTypes.LONG
            ));
        }
    }
}
