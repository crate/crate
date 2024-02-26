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

package io.crate.expression.scalar.systeminformation;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctions;
import io.crate.metadata.FunctionName;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.protocols.postgres.types.PGTypes;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

public final class FormatTypeFunction extends Scalar<String, Object> {

    private static final String NAME = "format_type";
    private static final FunctionName FQN = new FunctionName(PgCatalogSchemaInfo.NAME, NAME);

    public static void register(ScalarFunctions module) {
        module.register(
            Signature.scalar(
                FQN,
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ).withFeature(Feature.NULLABLE),
            FormatTypeFunction::new
        );
    }

    public FormatTypeFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    @SafeVarargs
    public final String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>... args) {
        var typeOid = (Integer) args[0].value();
        if (typeOid == null) {
            return null;
        }
        var type = PGTypes.fromOID(typeOid);
        if (type == null) {
            return "???";
        } else {
            int dimensions = 0;
            while (type instanceof ArrayType) {
                type = ((ArrayType<?>) type).innerType();
                dimensions++;
            }
            if (dimensions == 0) {
                return type.getName();
            }
            var sb = new StringBuilder();
            sb.append(type.getName());
            for (int i = 0; i < dimensions; i++) {
                sb.append("[]");
            }
            return sb.toString();
        }
    }
}
