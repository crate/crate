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

package io.crate.expression.udf;

import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;

import javax.annotation.Nullable;
import javax.script.ScriptException;


/**
 * Common interface for languages for user-defined functions.
 *
 * A language must be registered with the {@link UserDefinedFunctionService} in order to be able to create functions
 * based on that language.
 *
 * A language must provide a unique name which is exposed via the {@link #name()} method.
 * It is also responsible for validating the function script provided by the meta data
 * as well as for creating the function implementation from the meta data.
 */
public interface UDFLanguage {

    /**
     * Create the function implementation for a function from its meta data.
     * @param metaData from the cluster state
     * @return the function implementation
     * @throws ScriptException if the implementation cannot be created
     */
    Scalar createFunctionImplementation(UserDefinedFunctionMetaData metaData, Signature signature) throws ScriptException;

    /**
     * Validate the function code provided by the meta data.
     * @param metadata holding information about the user-defined function
     * @return error message if validation of the function fails, otherwise null
     */
    @Nullable
    String validate(UserDefinedFunctionMetaData metadata);

    /**
     * @return name of the language
     */
    String name();

}

