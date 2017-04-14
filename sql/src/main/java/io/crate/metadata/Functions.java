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
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Functions {

    private final Map<String, FunctionResolver> functionResolvers;
    private final Map<String, Map<String, FunctionResolver>> udfResolversBySchema = new ConcurrentHashMap<>();

    @Inject
    public Functions(Map<FunctionIdent, FunctionImplementation> functionImplementations,
                     Map<String, FunctionResolver> functionResolvers) {
        this.functionResolvers = Maps.newHashMap(functionResolvers);
        this.functionResolvers.putAll(generateFunctionResolvers(functionImplementations));
    }

    private Map<String, FunctionResolver> generateFunctionResolvers(Map<FunctionIdent, FunctionImplementation> functionImplementations) {
        Multimap<String, Tuple<FunctionIdent, FunctionImplementation>> signatures = getSignatures(functionImplementations);
        return signatures.keys().stream()
            .distinct()
            .collect(Collectors.toMap(name -> name, name -> new GeneratedFunctionResolver(signatures.get(name))));
    }

    private Multimap<String, Tuple<FunctionIdent, FunctionImplementation>> getSignatures(
        Map<FunctionIdent, FunctionImplementation> functionImplementations) {
        Multimap<String, Tuple<FunctionIdent, FunctionImplementation>> signatureMap = ArrayListMultimap.create();
        for (Map.Entry<FunctionIdent, FunctionImplementation> entry : functionImplementations.entrySet()) {
            signatureMap.put(entry.getKey().name(), new Tuple<>(entry.getKey(), entry.getValue()));
        }
        return signatureMap;
    }

    public void registerUdfResolversForSchema(String schema, Map<FunctionIdent, FunctionImplementation> functions) {
        udfResolversBySchema.put(schema, generateFunctionResolvers(functions));
    }

    public void deregisterUdfResolversForSchema(String schema) {
        udfResolversBySchema.remove(schema);
    }

    @Nullable
    private static FunctionImplementation resolveFunctionForArgumentTypes(List<DataType> types, FunctionResolver resolver) {
        List<DataType> signature = resolver.getSignature(types);
        if (signature != null) {
            return resolver.getForTypes(signature);
        }
        return null;
    }

    /**
     * Returns the built-in function implementation for the given function name and arguments.
     *
     * @param name The function name.
     * @param argumentsTypes The function argument types.
     * @return a function implementation or null if it was not found.
     */
    @Nullable
    public FunctionImplementation getBuiltin(String name, List<DataType> argumentsTypes) {
        FunctionResolver resolver = functionResolvers.get(name);
        if (resolver == null) {
            return null;
        }
        return resolveFunctionForArgumentTypes(argumentsTypes, resolver);
    }

    /**
     * Returns the user-defined function implementation for the given function name and arguments.
     *
     * @param name The function name.
     * @param argumentsTypes The function argument types.
     * @return a function implementation.
     * @throws UnsupportedOperationException if no implementation is found.
     */
    public FunctionImplementation getUserDefined(String schema, String name, List<DataType> argumentsTypes) throws UnsupportedOperationException {
        Map<String, FunctionResolver> functionResolvers = udfResolversBySchema.get(schema);
        if (functionResolvers == null) {
            throw createUnknownFunctionException(name, argumentsTypes);
        }
        FunctionResolver resolver = functionResolvers.get(name);
        if (resolver == null) {
            throw createUnknownFunctionException(name, argumentsTypes);
        }
        FunctionImplementation impl = resolveFunctionForArgumentTypes(argumentsTypes, resolver);
        if (impl == null) {
            throw createUnknownFunctionException(name, argumentsTypes);
        }
        return impl;
    }

    /**
     * Returns the function implementation for the given function ident.
     * First look up function in built-ins then fallback to user-defined functions.
     *
     * @param ident The function ident.
     * @return The function implementation.
     * @throws UnsupportedOperationException if no implementation is found.
     */
    public FunctionImplementation getQualified(FunctionIdent ident) throws UnsupportedOperationException {
        FunctionImplementation impl = getBuiltin(ident.name(), ident.argumentTypes());
        if (impl == null) {
            if (ident.schema() == null) {
                throw createUnknownFunctionException(ident.name(), ident.argumentTypes());
            }
            impl = getUserDefined(ident.schema(), ident.name(), ident.argumentTypes());
        }
        return impl;
    }

    public static UnsupportedOperationException createUnknownFunctionException(String name, List<DataType> argumentTypes) {
        return new UnsupportedOperationException(
            String.format(Locale.ENGLISH, "unknown function: %s(%s)", name, Joiner.on(", ").join(argumentTypes))
        );
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
