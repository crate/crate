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
import io.crate.types.DataTypes;
import io.crate.types.UndefinedType;
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
     * <p>
     * returns the functionImplementation for the given ident.
     * </p>
     * <p>
     * same as {@link #get(FunctionIdent)} but will throw an UnsupportedOperationException
     * if no implementation is found.
     */
    public FunctionImplementation getSafe(FunctionIdent ident)
        throws IllegalArgumentException, UnsupportedOperationException {
        FunctionImplementation implementation = null;
        String exceptionMessage = null;
        try {
            implementation = get(ident);
        } catch (IllegalArgumentException e) {
            if (e.getMessage() != null && !e.getMessage().isEmpty()) {
                exceptionMessage = e.getMessage();
            }
        }
        if (implementation == null) {
            if (exceptionMessage == null) {
                exceptionMessage = String.format(Locale.ENGLISH, "unknown function: %s(%s)", ident.name(),
                    Joiner.on(", ").join(ident.argumentTypes()));
            }
            throw new UnsupportedOperationException(exceptionMessage);
        }
        return implementation;
    }

    /**
     * returns the functionImplementation for the given ident
     * or null if nothing was found
     */
    @Nullable
    public FunctionImplementation get(FunctionIdent ident) throws IllegalArgumentException {
        FunctionResolver dynamicResolver = functionResolvers.get(ident.name());
        if (dynamicResolver != null) {
            List<DataType> argumentTypes = ident.argumentTypes();
            Signature signature = getMatchingSignature(argumentTypes, dynamicResolver);
            if (signature != null) {
                return dynamicResolver.getForTypes(replaceUndefinedDataTypesIfPossible(argumentTypes, signature));
            }
        }
        return null;
    }

    /**
     * Returns a possible matching {@link Signature} provided by a given {@link FunctionResolver}
     */
    @Nullable
    private Signature getMatchingSignature(List<DataType> argumentTypes, FunctionResolver resolver) {
        for (Signature signature : resolver.signatures()) {
            if (signature.matches(argumentTypes)) {
                return signature;
            }
        }
        return null;
    }

    /**
     * Replace possible {@link UndefinedType} elements with corresponding element at the matching signature.
     * This will ensure, a function will be resolved with correct types.
     *
     * <p><strong>Note that if the corresponding element at the signature is an ANY type, it won't be used as a
     * replacement for the {@link UndefinedType} as ANY types must only be used for signature matching.</strong></p>
     */
    private List<DataType> replaceUndefinedDataTypesIfPossible(List<DataType> argTypes, Signature matchingSignature) {
        ListIterator<DataType> argsIt = argTypes.listIterator();
        int signatureSize = matchingSignature.size();
        boolean replacementNeeded = false;

        // detect if any replacement is needed
        while (argsIt.hasNext()) {
            int i = argsIt.nextIndex();
            DataType dt = argsIt.next();
            if (i < signatureSize && dt.id() == UndefinedType.ID) {
                DataType replacedDt = matchingSignature.get(i);
                if (!DataTypes.isAnyOrAnyCollection(replacedDt)) {
                    replacementNeeded = true;
                    break;
                }
            }
        }
        if (!replacementNeeded) {
            return argTypes;
        }

        // do the replacement. we must create a new list here, as we can't be sure the incoming list is mutable
        List<DataType> newArgumentTypes = new ArrayList<>(argTypes.size());
        argsIt = argTypes.listIterator();
        while (argsIt.hasNext()) {
            int i = argsIt.nextIndex();
            DataType dt = argsIt.next();
            if (i < signatureSize && dt.id() == UndefinedType.ID) {
                DataType replacedDt = matchingSignature.get(i);
                if (!DataTypes.isAnyOrAnyCollection(replacedDt)) {
                    newArgumentTypes.add(replacedDt);
                    continue;
                }
            }
            newArgumentTypes.add(dt);
        }
        return newArgumentTypes;
    }

    private static class GeneratedFunctionResolver implements FunctionResolver {

        private final List<Signature> signatures;
        private final Map<List<DataType>, FunctionImplementation> functions;

        GeneratedFunctionResolver(Collection<Tuple<FunctionIdent, FunctionImplementation>> functionTuples) {
            signatures = new ArrayList<>(functionTuples.size());
            functions = new HashMap<>(functionTuples.size());
            for (Tuple<FunctionIdent, FunctionImplementation> functionTuple : functionTuples) {
                List<DataType> argumentTypes = functionTuple.v1().argumentTypes();
                signatures.add(new Signature(argumentTypes));
                functions.put(argumentTypes, functionTuple.v2());
            }
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            return functions.get(dataTypes);
        }

        @Override
        public List<Signature> signatures() {
            return signatures;
        }
    }
}
