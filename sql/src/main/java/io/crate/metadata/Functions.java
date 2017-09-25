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

package io.crate.metadata;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import io.crate.analyze.symbol.FuncArg;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.types.DataType;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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

    /**
     * Adds all provided {@link FunctionIdent} to a Multimap with the function
     * name as key and all possible overloads as values.
     * @param functionImplementations A map of all {@link FunctionIdent}.
     * @return The MultiMap with the function name as key and a tuple of
     *         FunctionIdent and FunctionImplementation as value.
     */
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
    private static FunctionImplementation resolveFunctionForArgumentTypes(List<? extends FuncArg> types,
                                                                          FunctionResolver resolver) {
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
     * @param dataTypes The function argument types.
     * @return a function implementation or null if it was not found.
     */
    @Nullable
    public FunctionImplementation getBuiltin(String name, List<DataType> dataTypes) {
        FunctionResolver resolver = lookupBuiltinFunctionResolver(name);
        if (resolver == null) {
            return null;
        }
        return resolver.getForTypes(dataTypes);
    }

    /**
     * Returns the built-in function implementation for the given function name and argument types.
     * The types may be cast to match the built-in argument types.
     *
     * @param name The function name.
     * @param argumentsTypes The function argument types.
     * @return a function implementation or null if it was not found.
     */
    @Nullable
    public FunctionImplementation getBuiltinByArgs(String name, List<? extends FuncArg> argumentsTypes) {
        FunctionResolver resolver = lookupBuiltinFunctionResolver(name);
        if (resolver == null) {
            return null;
        }
        return resolveFunctionForArgumentTypes(argumentsTypes, resolver);
    }

    /**
     * Returns the user-defined function implementation for the given function name and argTypes.
     *
     * @param name The function name.
     * @param argTypes The function argTypes.
     * @return a function implementation.
     */
    @Nullable
    public FunctionImplementation getUserDefined(String schema,
                                                 String name,
                                                 List<DataType> argTypes) throws UnsupportedOperationException {
        FunctionResolver resolver = lookupUdfFunctionResolver(schema, name, argTypes);
        if (resolver == null) {
            return null;
        }
        return resolver.getForTypes(argTypes);
    }

    /**
     * Returns the user-defined function implementation for the given function name and arguments.
     * The types may be cast to match the built-in argument types.
     *
     * @param name The function name.
     * @param arguments The function arguments.
     * @return a function implementation.
     */
    @Nullable
    public FunctionImplementation getUserDefinedByArgs(String schema,
                                                       String name,
                                                       List<? extends FuncArg> arguments) throws UnsupportedOperationException {
        FunctionResolver resolver = lookupUdfFunctionResolver(schema, name, arguments);
        if (resolver == null) {
            return null;
        }
        return resolveFunctionForArgumentTypes(arguments, resolver);
    }

    @Nullable
    private FunctionResolver lookupBuiltinFunctionResolver(String name) {
        return functionResolvers.get(name);
    }

    @Nullable
    private FunctionResolver lookupUdfFunctionResolver(String schema, String name, List<?> arguments) {
        Map<String, FunctionResolver> functionResolvers = udfResolversBySchema.get(schema);
        if (functionResolvers == null) {
            return null;
        }
        return functionResolvers.get(name);
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
        FunctionImplementation impl = null;
        if (ident.schema() == null) {
            impl = getBuiltin(ident.name(), ident.argumentTypes());
        }
        if (impl == null) {
            impl = getUserDefined(ident.schema(), ident.name(), ident.argumentTypes());
        }
        return impl;
    }

    public static UnsupportedOperationException createUnknownFunctionExceptionFromArgs(String name,
                                                                                       List<? extends FuncArg> arguments) {
        List<DataType> dataTypes = arguments.stream().map(FuncArg::valueType).collect(Collectors.toList());
        return createUnknownFunctionException(name, dataTypes);
    }

    public static UnsupportedOperationException createUnknownFunctionException(String name, List<DataType> arguments) {
        return new UnsupportedOperationException(
            String.format(Locale.ENGLISH, "unknown function: %s(%s)", name, Joiner.on(", ").join(arguments))
        );
    }

    private static class GeneratedFunctionResolver implements FunctionResolver {

        private final Map<Integer, FuncParams> allFuncParams;
        private final Map<List<DataType>, FunctionImplementation> functions;

        GeneratedFunctionResolver(Collection<Tuple<FunctionIdent, FunctionImplementation>> functionTuples) {
            functions = new HashMap<>(functionTuples.size());

            Map<Integer, FuncParams.Builder> funcParamsBuilders = new HashMap<>();

            for (Tuple<FunctionIdent, FunctionImplementation> functionTuple : functionTuples) {
                List<DataType> argumentTypes = functionTuple.v1().argumentTypes();
                functions.put(argumentTypes, functionTuple.v2());

                FuncParams.Builder funcParamsBuilder = funcParamsBuilders.get(argumentTypes.size());
                if (funcParamsBuilder == null) {
                    funcParamsBuilders.put(argumentTypes.size(), FuncParams.builder(argumentTypes));
                } else {
                    funcParamsBuilder.mergeWithTypes(argumentTypes);
                }
            }

            allFuncParams = new HashMap<>(funcParamsBuilders.size());
            funcParamsBuilders.forEach((numArgs, builder) -> allFuncParams.put(numArgs, builder.build()));
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            return functions.get(dataTypes);
        }

        @Nullable
        @Override
        public List<DataType> getSignature(List<? extends FuncArg> funcArgs) {
            FuncParams funcParams = allFuncParams.get(funcArgs.size());
            if (funcParams != null) {
                List<DataType> sig = funcParams.match(funcArgs);
                if (sig != null) {
                    return sig;
                }
            }
            return null;
        }
    }
}
