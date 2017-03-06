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
import java.util.stream.Collectors;

public class Functions {

    private final Map<String, FunctionResolver> functionResolvers;
    private final Map<String, Map<String, FunctionResolver>> schemaFunctionResolvers;

    @Inject
    public Functions(Map<FunctionIdent, FunctionImplementation> functionImplementations,
                     Map<String, FunctionResolver> functionResolvers) {
        this.functionResolvers = Maps.newHashMap(functionResolvers);
        this.functionResolvers.putAll(generateFunctionResolvers(functionImplementations));
        schemaFunctionResolvers = new HashMap<>();
    }

    public Map<String, FunctionResolver> generateFunctionResolvers(Map<FunctionIdent, FunctionImplementation> functionImplementations) {
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

    public void registerSchemaFunctionResolvers(String schema, Map<String, FunctionResolver> resolvers) {
        schemaFunctionResolvers.put(schema, resolvers);
    }

    public void deregisterSchemaFunctionResolvers(String schema) {
        schemaFunctionResolvers.remove(schema);
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
            FunctionImplementation function = getFunctionImplementation(ident, dynamicResolver);
            if (function != null) return function;
        }

        FunctionResolver schemaResolver = getSchemaFunctionResolver(ident);
        if (schemaResolver != null) {
            return getFunctionImplementation(ident, schemaResolver);
        }
        return null;
    }

    private FunctionImplementation getFunctionImplementation(FunctionIdent ident, FunctionResolver dynamicResolver) {
        List<DataType> argumentTypes = ident.argumentTypes();
        List<DataType> signature = dynamicResolver.getSignature(argumentTypes);
        if (signature != null) {
            return dynamicResolver.getForTypes(signature);
        }
        return null;
    }

    private FunctionResolver getSchemaFunctionResolver(FunctionIdent ident) {
        String[] parts = getSchemaFunctionName(ident);
        Map<String, FunctionResolver> schemaFunctionResolver = schemaFunctionResolvers.get(parts[0]);
        if (schemaFunctionResolver != null) {
            return schemaFunctionResolver.get(Joiner.on(".").join(parts));
        }
        return null;
    }

    private static String[] getSchemaFunctionName(FunctionIdent ident) {
        String[] parts = ident.name().split("\\.");
        if (parts.length == 1) {
            return new String[]{Schemas.DEFAULT_SCHEMA_NAME, parts[0]};
        }
        return parts;
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
