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
import io.crate.common.collections.Lists2;
import io.crate.expression.symbol.FuncArg;
import io.crate.metadata.functions.BoundVariables;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.SignatureBinder;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.crate.common.collections.Lists2.getOnlyElement;

public class Functions {

    private static final Logger LOGGER = Loggers.getLogger(Functions.class);

    private final Map<FunctionName, FunctionResolver> functionResolvers;
    private final Map<FunctionName, List<FuncResolver>> udfFunctionImplementations = new ConcurrentHashMap<>();
    private final Map<FunctionName, List<FuncResolver>> functionImplementations;

    @Inject
    public Functions(Map<FunctionIdent, FunctionImplementation> functionImplementations,
                     Map<FunctionName, FunctionResolver> functionResolvers,
                     Map<FunctionName, List<FuncResolver>> functionImplementationsBySignature) {
        this.functionResolvers = Maps.newHashMap(functionResolvers);
        this.functionResolvers.putAll(generateFunctionResolvers(functionImplementations));
        this.functionImplementations = functionImplementationsBySignature;
    }

    public Functions(Map<FunctionIdent, FunctionImplementation> functionImplementations,
                     Map<FunctionName, FunctionResolver> functionResolvers) {
        this(functionImplementations, functionResolvers, Collections.emptyMap());
    }

    public Map<FunctionName, FunctionResolver> functionResolvers() {
        return functionResolvers;
    }

    public Map<FunctionName, List<FuncResolver>> udfFunctionResolvers() {
        return udfFunctionImplementations;
    }

    private Map<FunctionName, FunctionResolver> generateFunctionResolvers(Map<FunctionIdent, FunctionImplementation> functionImplementations) {
        Multimap<FunctionName, Tuple<FunctionIdent, FunctionImplementation>> signatures = getSignatures(functionImplementations);
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
    private Multimap<FunctionName, Tuple<FunctionIdent, FunctionImplementation>> getSignatures(
        Map<FunctionIdent, FunctionImplementation> functionImplementations) {
        Multimap<FunctionName, Tuple<FunctionIdent, FunctionImplementation>> signatureMap = ArrayListMultimap.create();
        for (Map.Entry<FunctionIdent, FunctionImplementation> entry : functionImplementations.entrySet()) {
            signatureMap.put(entry.getKey().fqnName(), new Tuple<>(entry.getKey(), entry.getValue()));
        }
        return signatureMap;
    }

    public void registerUdfFunctionImplementationsForSchema(
        String schema, Map<FunctionName, List<FuncResolver>> functions) {
        // remove deleted ones before re-registering all current ones for the given schema
        udfFunctionImplementations.entrySet()
            .removeIf(
                function ->
                    schema.equals(function.getKey().schema())
                    && functions.get(function.getKey()) == null);
        udfFunctionImplementations.putAll(functions);
    }

    public void deregisterUdfResolversForSchema(String schema) {
        udfFunctionImplementations.keySet()
            .removeIf(function -> schema.equals(function.schema()));
    }

    /**
     * Return a function that matches the name/arguments.
     *
     * <pre>
     * {@code
     * Lookup logic:
     *     No schema:   Built-ins -> Function or UDFs in searchPath
     *     With Schema: Function or UDFs in schema
     * }
     * </pre>
     *
     * @throws UnsupportedOperationException if the function wasn't found
     */
    public FunctionImplementation get(@Nullable String suppliedSchema,
                                      String functionName,
                                      List<? extends FuncArg> arguments,
                                      SearchPath searchPath) {
        FunctionName fqnName = new FunctionName(suppliedSchema, functionName);
        FunctionImplementation func = getBuiltinByArgs(fqnName, arguments, searchPath);
        if (func == null) {
            func = resolveUserDefinedByArgs(fqnName, arguments, searchPath);
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
     * @param functionName The full qualified function name.
     * @param dataTypes The function argument types.
     * @return a function implementation or null if it was not found.
     */
    @Nullable
    private FunctionImplementation getBuiltin(FunctionName functionName, List<DataType> dataTypes) {
        FunctionResolver resolver = functionResolvers.get(functionName);
        if (resolver == null) {
            return null;
        }
        return resolver.getForTypes(dataTypes);
    }

    @Nullable
    private FunctionImplementation get(Signature signature,
                                       List<DataType> actualArgumentTypes,
                                       Function<FunctionName, List<FuncResolver>> lookupFunction) {
        var candidates = lookupFunction.apply(signature.getName());
        if (candidates == null) {
            return null;
        }
        for (var candidate : candidates) {
            if (candidate.getSignature().equals(signature)) {
                return candidate.getFactory().apply(signature, actualArgumentTypes);
            }
        }
        return null;
    }

    /**
     * Returns the built-in function implementation for the given function name and argument types.
     * The types may be cast to match the built-in argument types.
     *
     * @param functionName The full qualified function name.
     * @param argumentsTypes The function argument types.
     * @return a function implementation or null if it was not found.
     */
    @Nullable
    private FunctionImplementation getBuiltinByArgs(FunctionName functionName,
                                                    List<? extends FuncArg> argumentsTypes,
                                                    SearchPath searchPath) {
        // V2
        FunctionImplementation impl = resolveBuiltInFunctionBySignature(
            functionName,
            Lists2.map(argumentsTypes, FuncArg::valueType),
            searchPath
        );
        if (impl != null) {
            return impl;
        }

        FunctionResolver resolver = lookupFunctionResolver(functionName, searchPath, functionResolvers::get);
        if (resolver == null) {
            return null;
        }
        return resolveFunctionForArgumentTypes(argumentsTypes, resolver);
    }

    @Nullable
    public FunctionImplementation resolveBuiltInFunctionBySignature(FunctionName name,
                                                                     List<DataType> arguments,
                                                                     SearchPath searchPath) {
        return resolveFunctionBySignature(name, arguments, searchPath, functionImplementations::get);
    }


    @Nullable
    private static FunctionImplementation resolveFunctionBySignature(FunctionName name,
                                                                     List<DataType> arguments,
                                                                     SearchPath searchPath,
                                                                     Function<FunctionName, List<FuncResolver>> lookupFunction) {
        var candidates = lookupFunction.apply(name);
        if (candidates == null && name.schema() == null) {
            for (String pathSchema : searchPath) {
                FunctionName searchPathFunctionName = new FunctionName(pathSchema, name.name());
                candidates = lookupFunction.apply(searchPathFunctionName);
                if (candidates != null) {
                    break;
                }
            }
        }
        if (candidates != null) {
            assert candidates.stream().allMatch(f -> f.getSignature().getBindingInfo() != null) :
                "Resolving/Matching of signatures can only be done with non-null signature's binding info";

            @SuppressWarnings("ConstantConditions")
            // First lets try exact candidates, no generic type variables, no coercion allowed.
            var exactCandidates = candidates.stream()
                .filter(function -> function.getSignature().getBindingInfo().getTypeVariableConstraints().isEmpty())
                .collect(Collectors.toList());
            var match = matchFunctionCandidates(exactCandidates, arguments, false);
            if (match != null) {
                return match;
            }

            @SuppressWarnings("ConstantConditions")
            // Second, try candidates with generic type variables, still no coercion allowed.
            var genericCandidates = candidates.stream()
                .filter(function -> !function.getSignature().getBindingInfo().getTypeVariableConstraints().isEmpty())
                .collect(Collectors.toList());
            match = matchFunctionCandidates(genericCandidates, arguments, false);
            if (match != null) {
                return match;
            }

            @SuppressWarnings("ConstantConditions")
            // Last, try all candidates which allow coercion.
            var candidatesAllowingCoercion = candidates.stream()
                .filter(function -> function.getSignature().getBindingInfo().isCoercionAllowed())
                .collect(Collectors.toList());
            return matchFunctionCandidates(candidatesAllowingCoercion, arguments, true);
        }
        return null;
    }

    @Nullable
    private static FunctionImplementation matchFunctionCandidates(List<FuncResolver> candidates,
                                                                  List<DataType> argumentTypes,
                                                                  boolean allowCoercion) {
        List<ApplicableFunction> applicableFunctions = new ArrayList<>();
        for (FuncResolver candidate : candidates) {
            Signature boundSignature = new SignatureBinder(candidate.getSignature(), allowCoercion)
                .bind(Lists2.map(argumentTypes, DataType::getTypeSignature));
            if (boundSignature != null) {
                applicableFunctions.add(
                    new ApplicableFunction(
                        candidate.getSignature(),
                        boundSignature,
                        candidate.getFactory()
                    )
                );
            }
        }


        if (allowCoercion) {
            applicableFunctions = selectMostSpecificFunctions(applicableFunctions, argumentTypes);
            if (LOGGER.isDebugEnabled() && applicableFunctions.isEmpty()) {
                LOGGER.debug("At least single function must be left after selecting most specific one");
            }
        }

        if (applicableFunctions.size() == 1) {
            return getOnlyElement(applicableFunctions).get();
        }
        if (applicableFunctions.size() > 1) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Multiple candidates match! " + applicableFunctions);
            }
        }

        return null;
    }


    /**
     * Returns the user-defined function implementation for the given function name and argTypes.
     *
     * @param functionName The full qualified function name.
     * @param argTypes The function argTypes.
     * @return a function implementation.
     */
    @Nullable
    private FunctionImplementation getUserDefined(FunctionName functionName,
                                                  List<DataType> argTypes) throws UnsupportedOperationException {
        return resolveFunctionBySignature(
            functionName,
            argTypes,
            SearchPath.pathWithPGCatalogAndDoc(),
            udfFunctionImplementations::get
        );
    }

    /**
     * Returns the user-defined function implementation for the given function name and arguments.
     * The types may be cast to match the built-in argument types.
     *
     * @param functionName The full qualified function name.
     * @param argumentsTypes The function arguments.
     * @param searchPath The {@link SearchPath} against which to try to resolve the function if it is not identified by
     *                   a fully qualifed name (ie. `schema.functionName`)
     * @return a function implementation.
     */
    @Nullable
    private FunctionImplementation resolveUserDefinedByArgs(FunctionName functionName,
                                                            List<? extends FuncArg> argumentsTypes,
                                                            SearchPath searchPath) throws UnsupportedOperationException {
        return resolveFunctionBySignature(
            functionName,
            Lists2.map(argumentsTypes, FuncArg::valueType),
            searchPath,
            udfFunctionImplementations::get
        );
    }

    @Nullable
    private static FunctionResolver lookupFunctionResolver(FunctionName functionName,
                                                           Iterable<String> searchPath,
                                                           Function<FunctionName, FunctionResolver> lookupFunction) {
        FunctionResolver functionResolver = lookupFunction.apply(functionName);
        if (functionResolver == null && functionName.schema() == null) {
            for (String pathSchema : searchPath) {
                FunctionName searchPathfunctionName = new FunctionName(pathSchema, functionName.name());
                functionResolver = lookupFunction.apply(searchPathfunctionName);
                if (functionResolver != null) {
                    break;
                }
            }
        }
        return functionResolver;
    }

    /**
     * @deprecated Superseded by {@link #getQualified(Signature, List)}.
     *             This method gets removed once all functions use the signature based registry.
     *
     * Returns the function implementation for the given function ident.
     * First look up function in built-ins then fallback to user-defined functions.
     *
     * @param ident The function ident.
     * @return The function implementation.
     * @throws UnsupportedOperationException if no implementation is found.
     */
    public FunctionImplementation getQualified(FunctionIdent ident) throws UnsupportedOperationException {
        FunctionImplementation impl = getBuiltin(ident.fqnName(), ident.argumentTypes());
        if (impl == null) {
            impl = getUserDefined(ident.fqnName(), ident.argumentTypes());
        }
        return impl;
    }

    /**
     * Returns the function implementation for the given function signature.
     * First look up function in built-ins then fallback to user-defined functions.
     *
     * @param signature The function signature.
     * @return The function implementation.
     * @throws UnsupportedOperationException if no implementation is found.
     */
    public FunctionImplementation getQualified(Signature signature,
                                               List<DataType> actualArgumentTypes) throws UnsupportedOperationException {
        FunctionImplementation impl = get(signature, actualArgumentTypes, functionImplementations::get);
        if (impl == null) {
            impl = get(signature, actualArgumentTypes, udfFunctionImplementations::get);
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

    private static List<ApplicableFunction> selectMostSpecificFunctions(List<ApplicableFunction> applicableFunctions,
                                                                        List<DataType> argumentTypes) {
        if (applicableFunctions.isEmpty()) {
            return applicableFunctions;
        }

        // Find most specific by number of exact argument type matches
        List<TypeSignature> argumentTypeSignatures = Lists2.map(argumentTypes, DataType::getTypeSignature);
        List<ApplicableFunction> mostSpecificFunctions = selectMostSpecificFunctions(
            applicableFunctions,
            (l, r) -> hasMoreExactTypeMatches(l, r, argumentTypeSignatures));
        if (mostSpecificFunctions.size() <= 1) {
            return mostSpecificFunctions;
        }

        // Find most specific by type precedence
        mostSpecificFunctions = selectMostSpecificFunctions(applicableFunctions, Functions::isMoreSpecificThan);
        if (mostSpecificFunctions.size() <= 1) {
            return mostSpecificFunctions;
        }

        // If the return type for all the selected function is the same
        // all the functions are semantically the same. We can return just any of those.
        //
        // Second, if all given arguments are UNDEFINED, we also return the first one.
        // This may not be 100% correct, but e.g. for
        //
        //   `concat(null, null)`
        //
        // we currently have no other option as we cannot specify which one should match by signatures.
        //
        //     `concat(text, text):text`
        //     `concat(array(E), array(E)):array(E)`
        //
        if (returnTypeIsTheSame(mostSpecificFunctions)
            || argumentTypes.stream().allMatch(d -> d.id() == DataTypes.UNDEFINED.id())) {
            ApplicableFunction selectedFunction = mostSpecificFunctions.stream()
                .sorted(Comparator.comparing(Objects::toString))
                .iterator().next();

            return List.of(selectedFunction);
        }

        return mostSpecificFunctions;
    }

    private static List<ApplicableFunction> selectMostSpecificFunctions(
        List<ApplicableFunction> candidates,
        BiFunction<ApplicableFunction, ApplicableFunction, Boolean> isMoreSpecific) {
        List<ApplicableFunction> representatives = new ArrayList<>();

        for (ApplicableFunction current : candidates) {
            boolean found = false;
            for (int i = 0; i < representatives.size(); i++) {
                ApplicableFunction representative = representatives.get(i);
                if (isMoreSpecific.apply(current, representative)) {
                    representatives.clear();
                    representatives.add(current);
                    found = true;
                    break;
                } else if (isMoreSpecific.apply(representative, current)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                representatives.add(current);
            }
        }

        return representatives;
    }

    /**
     * One method is more specific than another if invocation handled by the first method
     * could be passed on to the other one.
     * Additionally possible variadic type signatures are taken into account,
     * an exact amount of declared type signatures is more specific than expanded variadic type signatures.
     */
    private static boolean isMoreSpecificThan(ApplicableFunction left,
                                              ApplicableFunction right) {
        List<TypeSignature> resolvedTypes = left.getBoundSignature().getArgumentTypes();
        BoundVariables boundVariables = SignatureBinder.withPrecedenceOnly(right.getDeclaredSignature())
            .bindVariables(resolvedTypes);
        if (boundVariables == null) {
            return false;
        }

        int leftArgsCount = left.getDeclaredSignature().getArgumentTypes().size();
        int rightArgsCount = right.getDeclaredSignature().getArgumentTypes().size();
        return leftArgsCount >= rightArgsCount;
    }

    private static boolean hasMoreExactTypeMatches(ApplicableFunction left,
                                                   ApplicableFunction right,
                                                   List<TypeSignature> actualArgumentTypes) {
        int leftExactMatches = numberOfExactTypeMatches(
            actualArgumentTypes,
            left.getBoundSignature().getArgumentTypes()
        );
        int rightExactMatches = numberOfExactTypeMatches(
            actualArgumentTypes,
            right.getBoundSignature().getArgumentTypes()
        );
        return leftExactMatches > rightExactMatches;
    }

    private static boolean returnTypeIsTheSame(List<ApplicableFunction> applicableFunctions) {
        Set<DataType<?>> returnTypes = applicableFunctions.stream()
            .map(function -> function.getBoundSignature().getReturnType().createType())
            .collect(Collectors.toSet());
        return returnTypes.size() == 1;
    }

    private static int numberOfExactTypeMatches(List<TypeSignature> actualArgumentTypes,
                                                List<TypeSignature> declaredArgumentTypes) {
        int cnt = 0;
        for (int i = 0; i < actualArgumentTypes.size(); i++) {
            if (actualArgumentTypes.get(i).equals(declaredArgumentTypes.get(i))) {
                cnt++;
            }
        }
        return cnt;
    }

    private static class ApplicableFunction implements Supplier<FunctionImplementation> {

        private final Signature declaredSignature;
        private final Signature boundSignature;
        private final BiFunction<Signature, List<DataType>, FunctionImplementation> factory;

        public ApplicableFunction(Signature declaredSignature,
                                  Signature boundSignature,
                                  BiFunction<Signature, List<DataType>, FunctionImplementation> factory) {
            this.declaredSignature = declaredSignature;
            this.boundSignature = boundSignature;
            this.factory = factory;
        }

        public Signature getDeclaredSignature() {
            return declaredSignature;
        }

        public Signature getBoundSignature() {
            return boundSignature;
        }

        @Override
        public FunctionImplementation get() {
            return factory.apply(
                declaredSignature,
                Lists2.map(boundSignature.getArgumentTypes(), TypeSignature::createType)
            );
        }

        @Override
        public String toString() {
            return "ApplicableFunction{" +
                   "declaredSignature=" + declaredSignature +
                   ", boundSignature=" + boundSignature +
                   '}';
        }
    }
}
