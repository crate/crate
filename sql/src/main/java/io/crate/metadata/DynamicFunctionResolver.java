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

import io.crate.analyze.symbol.Function;
import io.crate.types.DataType;

import java.util.List;

/**
 * resolver for methods that take a variable number of arguments or work with a lots of different arguments
 */
public interface DynamicFunctionResolver {

    /**
     * returns the function implementation for the given types.
     *
     * @throws java.lang.IllegalArgumentException thrown if there is no function that can handle the given types.
     */
    public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException;
}
