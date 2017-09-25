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

import io.crate.analyze.symbol.FuncArg;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Resolver for functions which allows to represent a variable number of arguments and argument types. Use this
 * resolver conjunction with {@link io.crate.metadata.functions.params.FuncParams} if possible.
 */
public interface FunctionResolver {

    /**
     * Checks the given data types if they match a signature and returns a normalized list of data types which should
     * be used as argument for {@link #getForTypes(List)} to get the actual implementation. If no match is found, null
     * is returned.
     *
     * The returned list might be the same as the input list if no normalization is needed, however the input list
     * is never modified and copied if a normalization occurs.
     *
     * Normalization means that types may be adjusted to match the signature of a function and undefined types
     * will become defined.
     *
     * @param funcArgs a list representing the types of the arguments
     * @return a normalized list of data types or null if no signature match is found
     */
    @Nullable
    List<DataType> getSignature(List<? extends FuncArg> funcArgs);

    /**
     * Returns the actual function implementation for the given argument type list.
     *
     * @throws java.lang.IllegalArgumentException thrown if there is no function that can handle the given types.
     */
    FunctionImplementation getForTypes(List<DataType> symbols) throws IllegalArgumentException;
}
