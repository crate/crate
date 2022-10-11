/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import java.util.function.BiFunction;

import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;

public class FunctionProvider {

    public interface FunctionFactory extends BiFunction<Signature, BoundSignature, FunctionImplementation> {
    }

    private final Signature signature;
    private final FunctionFactory factory;

    public FunctionProvider(Signature signature, FunctionFactory factory) {
        this.signature = signature;
        this.factory = factory;
    }

    public Signature getSignature() {
        return signature;
    }

    public FunctionFactory getFactory() {
        return factory;
    }

    @Override
    public String toString() {
        return "FunctionProvider{" + "signature=" + signature + '}';
    }
}
