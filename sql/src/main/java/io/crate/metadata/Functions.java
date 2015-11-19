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
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.Map;

public class Functions {

    private final Map<FunctionIdent, FunctionImplementation> functionImplementations;
    private final Map<String, DynamicFunctionResolver> functionResolvers;

    @Inject
    public Functions(Map<FunctionIdent, FunctionImplementation> functionImplementations,
                     Map<String, DynamicFunctionResolver> functionResolvers) {
        this.functionImplementations = functionImplementations;
        this.functionResolvers = functionResolvers;
    }

    /**
     * <p>
     *     returns the functionImplementation for the given ident.
     * </p>
     *
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
                exceptionMessage = String.format("unknown function: %s(%s)", ident.name(),
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
        FunctionImplementation implementation = functionImplementations.get(ident);
        if (implementation != null) {
            return implementation;
        }

        DynamicFunctionResolver dynamicResolver = functionResolvers.get(ident.name());
        if (dynamicResolver != null) {
            return dynamicResolver.getForTypes(ident.argumentTypes());
        }
        return null;
    }
}
