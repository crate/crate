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

import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Resolver for functions which allows to represent a variable number of arguments and arguement types. Use this
 * resolver if static registration by {@link FunctionIdent} is not possible or leads to a high number of permutations.
 */
public interface FunctionResolver {

    /**
     * Returns the actual function implementation for the given argument type list.
     *
     * @throws java.lang.IllegalArgumentException thrown if there is no function that can handle the given types.
     */
    FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException;

    /**
     * Checks the given data types if they match a signature and returns a normalized list of data types which should
     * be used as argument for {@link #getForTypes} to get the actual implementation. If not match is found null is
     * returned.
     *
     * The returned list might be the same as the input list if no normalization is needed, however the input list
     * is never modified and copied if a normalization occurs
     *
     * Normalization means that undefined types might become defined.
     *
     * @param dataTypes a list of data types representing the types of the arguments
     * @return a normalized list of data types or null if no signature match is found
     */
    @Nullable
    List<DataType> getSignature(List<DataType> dataTypes);
}
