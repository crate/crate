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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import io.crate.expression.symbol.FuncArg;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.types.DataType;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
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

    /**
     * Return a function that matches the name/arguments.
     *
     * <pre>
     * {@code
     * Lookup logic:
     *     No schema:   Built-ins -> UDFs in searchPath
     *     With Schema: UDFs in schema
     * }
     * </pre>
     *
     * @throws UnsupportedOperationException if the function wasn't found
     */
    public FunctionImplementation get(@Nullable String suppliedSchema,
                                      String functionName,
                                      List<? extends FuncArg> arguments,
                                      SearchPath searchPath) {
        FunctionImplementation func;
        if (suppliedSchema == null) {
            func = getBuiltinByArgs(functionName, arguments);
            if (func == null) {
                func = resolveUserDefinedByArgs(null, functionName, arguments, searchPath);
            }
        } else {
            func = resolveUserDefinedByArgs(suppliedSchema, functionName, arguments, searchPath);
        }
        if (func == null) {
            throw raiseUnknownFunction(suppliedSchema, functionName, arguments);
        }
        return func;
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
    private FunctionImplementation getBuiltin(String name, List<DataType> dataTypes) {
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
    private FunctionImplementation getBuiltinByArgs(String name, List<? extends FuncArg> argumentsTypes) {
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
    private FunctionImplementation getUserDefined(String schema,
                                                  String name,
                                                  List<DataType> argTypes) throws UnsupportedOperationException {
        FunctionResolver resolver = lookupUdfFunctionResolver(schema, name);
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
     * @param searchPath The {@link SearchPath} against which to try to resolve the function if it is not identified by
     *                   a fully qualifed name (ie. `schema.functionName`)
     * @return a function implementation.
     */
    @Nullable
    private FunctionImplementation resolveUserDefinedByArgs(@Nullable String schema,
                                                           String name,
                                                           List<? extends FuncArg> arguments,
                                                           SearchPath searchPath) throws UnsupportedOperationException {
        FunctionResolver resolver = lookupUdfFunctionResolver(schema, name, searchPath);
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
    private FunctionResolver lookupUdfFunctionResolver(String schema, String name) {
        Map<String, FunctionResolver> functionResolvers = udfResolversBySchema.get(schema);
        if (functionResolvers == null) {
            return null;
        }
        return functionResolvers.get(name);
    }

    @Nullable
    private FunctionResolver lookupUdfFunctionResolver(@Nullable String schema, String name, Iterable<String> searchPath) {
        if (schema != null) {
            return lookupUdfFunctionResolver(schema, name);
        } else {
            FunctionResolver functionResolver = null;
            for (String pathSchema : searchPath) {
                functionResolver = lookupUdfFunctionResolver(pathSchema, name);
                if (functionResolver != null) {
                    break;
                }
            }
            return functionResolver;
        }
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

    private static UnsupportedOperationException raiseUnknownFunction(@Nullable String suppliedSchema,
                                                                      String name,
                                                                      List<? extends FuncArg> arguments) {
        StringJoiner joiner = new StringJoiner(", ");
        for (FuncArg arg : arguments) {
            joiner.add(arg.valueType().toString());
        }
        String prefix = suppliedSchema == null ? "" : suppliedSchema + '.';
        throw new UnsupportedOperationException("unknown function: " + prefix + name + '(' + joiner.toString() + ')');
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
