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

package io.crate.expression.scalar;

import java.util.EnumSet;
import java.util.UUID;

import io.crate.data.Input;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.Signature.Feature;
import io.crate.types.UUIDType;

public class GenRandomUUIDFunction extends Scalar<UUID, Void> {

    private static final String NAME = "gen_random_uuid";

    private static void register(Functions.Builder builder, String name) {
        builder.add(
            Signature.builder(name, FunctionType.SCALAR)
                .argumentTypes()
                .returnType(UUIDType.INSTANCE.getTypeSignature())
                .features(EnumSet.of(Feature.NOTNULL))
                .build(),
            GenRandomUUIDFunction::new
        );
    }

    public static void register(Functions.Builder builder) {
        register(builder, NAME);
        register(builder, "uuidv4");
    }

    public GenRandomUUIDFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @SafeVarargs
    @Override
    public final UUID evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<Void>... args) {
        return UUID.randomUUID();
    }
}
