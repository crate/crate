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

package io.crate.metadata;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import io.crate.types.DataType;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.*;

public class Functions {

    private final Map<String, FunctionResolver> functionResolvers;

    @Inject
    public Functions(Map<FunctionIdent, FunctionImplementation> functionImplementations,
                     Map<String, FunctionResolver> functionResolvers) {
        this.functionResolvers = Maps.newHashMap(functionResolvers);
        generateFunctionResolvers(functionImplementations);
    }

    private void generateFunctionResolvers(Map<FunctionIdent, FunctionImplementation> functionImplementations) {
        Multimap<String, Tuple<FunctionIdent, FunctionImplementation>> signatureMap = ArrayListMultimap.create();
        for (Map.Entry<FunctionIdent, FunctionImplementation> entry : functionImplementations.entrySet()) {
            signatureMap.put(entry.getKey().name(), new Tuple<>(entry.getKey(), entry.getValue()));
        }
        for (String name : signatureMap.keys()) {
            functionResolvers.put(name, new GeneratedFunctionResolver(signatureMap.get(name)));
        }
    }

    /**
     * Returns the function implementation for the given function schema, name and arguments.
     * <p>
     *
     * @param schema         The schema name.
     * @param name           The function name.
     * @param argumentsTypes The function argument types.
     * @return a function implementation
     * @throws UnsupportedOperationException if an implementation is not found.
     */
    public FunctionImplementation getSafe(@Nullable String schema, String name, List<DataType> argumentsTypes)
        throws IllegalArgumentException, UnsupportedOperationException {
        FunctionImplementation implementation = null;
        String exceptionMessage = null;
        try {
            implementation = get(schema, name, argumentsTypes);
        } catch (IllegalArgumentException e) {
            if (e.getMessage() != null && !e.getMessage().isEmpty()) {
                exceptionMessage = e.getMessage();
            }
        }
        if (implementation == null) {
            if (exceptionMessage == null) {
                exceptionMessage = String.format(Locale.ENGLISH, "unknown function: %s(%s)", name,
                    Joiner.on(", ").join(argumentsTypes));
            }
            throw new UnsupportedOperationException(exceptionMessage);
        }
        return implementation;
    }

    /**
     * Returns the function implementation for the given function schema, name and arguments.
     * <p>
     *
     * @param schema         The schema name.
     * @param name           The function name.
     * @param argumentsTypes The function argument types.
     * @return a function implementation or null if nothing was found.
     */
    @Nullable
    public FunctionImplementation get(String schema, String name, List<DataType> argumentsTypes) throws
        IllegalArgumentException {
        FunctionResolver dynamicResolver = functionResolvers.get(name);
        if (dynamicResolver != null) {
            List<DataType> signature = dynamicResolver.getSignature(argumentsTypes);
            if (signature != null) {
                return dynamicResolver.getForTypes(signature);
            }
        }
        return null;
    }

    private static class GeneratedFunctionResolver implements FunctionResolver {

        private final List<Signature.SignatureOperator> signatures;
        private final Map<List<DataType>, FunctionImplementation> functions;

        GeneratedFunctionResolver(Collection<Tuple<FunctionIdent, FunctionImplementation>> functionTuples) {
            signatures = new ArrayList<>(functionTuples.size());
            functions = new HashMap<>(functionTuples.size());
            for (Tuple<FunctionIdent, FunctionImplementation> functionTuple : functionTuples) {
                List<DataType> argumentTypes = functionTuple.v1().argumentTypes();
                signatures.add(Signature.of(argumentTypes));
                functions.put(argumentTypes, functionTuple.v2());
            }
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            return functions.get(dataTypes);
        }

        @Nullable
        @Override
        public List<DataType> getSignature(List<DataType> dataTypes) {
            for (Signature.SignatureOperator signature : signatures) {
                List<DataType> sig = signature.apply(dataTypes);
                if (sig != null) {
                    return sig;
                }
            }
            return null;
        }
    }
}
