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

package io.crate.metadata.pgcatalog;

import static io.crate.metadata.FunctionType.AGGREGATE;
import static io.crate.metadata.FunctionType.WINDOW;
import static io.crate.metadata.pgcatalog.PgProcTable.Entry.pgTypeIdFrom;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.FLOAT;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.INTEGER_ARRAY;
import static io.crate.types.DataTypes.REGPROC;
import static io.crate.types.DataTypes.SHORT;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.STRING_ARRAY;

import java.util.ArrayList;
import java.util.Set;

import io.crate.common.collections.Lists;
import io.crate.metadata.FunctionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.metadata.functions.Signature;
import io.crate.protocols.postgres.types.AnyType;
import io.crate.protocols.postgres.types.PGArray;
import io.crate.protocols.postgres.types.PGTypes;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.Regproc;
import io.crate.types.RowType;
import io.crate.types.TypeSignature;

public final class PgProcTable {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_proc");

    private PgProcTable() {}

    public static SystemTable<Entry> create() {
        return SystemTable.<Entry>builder(IDENT)
            .add("oid", INTEGER, x -> OidHash.functionOid(x.signature))
            .add("proname", STRING, x -> x.signature.getName().name())
            .add("pronamespace", INTEGER, x -> OidHash.schemaOid(x.functionName.schema()))
            .add("proowner", INTEGER, x -> null)
            .add("prolang", INTEGER, x -> null)
            .add("procost", FLOAT, x -> null)
            .add("prorows", FLOAT, x -> !x.returnSetType ? 0f : 1000f)
            .add("provariadic", INTEGER, x -> {
                if (x.signature.getBindingInfo().isVariableArity()) {
                    var args = x.signature.getArgumentTypes();
                    return pgTypeIdFrom(args.get(args.size() - 1));
                } else {
                    return 0;
                }
            })
            .add("prosupport", REGPROC, x -> Regproc.REGPROC_ZERO)
            .add("prokind", STRING, x -> prokind(x.signature))
            .add("prosecdef", BOOLEAN, x -> null)
            .add("proleakproof", BOOLEAN, x -> null)
            .add("proisstrict", BOOLEAN, x -> null)
            .add("proretset", BOOLEAN, x -> x.returnSetType)
            .add("provolatile", STRING, x -> null)
            .add("proparallel", STRING, x -> null)
            .add("pronargs", SHORT, x -> (short) x.signature.getArgumentTypes().size())
            .add("pronargdefaults", SHORT, x -> null)
            .add("prorettype", INTEGER, x -> x.returnTypeId)
            .add("proargtypes", DataTypes.OIDVECTOR, x ->
                Lists.map(x.signature.getArgumentTypes(), Entry::pgTypeIdFrom))
            .add("proallargtypes", INTEGER_ARRAY, x -> null)
            .add("proargmodes", STRING_ARRAY, x -> {
                if (!x.signature.getBindingInfo().isVariableArity()) {
                    // return null because all arguments have in mode
                    return null;
                } else {
                    int numOfArgs = x.signature.getArgumentTypes().size();
                    var modes = new ArrayList<String>(numOfArgs);
                    for (int i = 0; i < numOfArgs - 1; i++) {
                        modes.add("i");
                    }
                    modes.add("v");
                    return modes;
                }
            })
            .add("proargnames", STRING_ARRAY, x -> null)
            .add("proargdefaults", STRING, x -> null)
            .add("protrftypes", INTEGER_ARRAY, x -> null)
            .add("prosrc", STRING, x -> x.functionName.name())
            .add("probin", STRING, x -> null)
            .add("prosqlbody", STRING, x -> null)
            .add("proconfig", STRING_ARRAY, x -> null)
            // should be `aclitem[]` but we lack `aclitem`, so going with same choice that Cockroach made:
            // https://github.com/cockroachdb/cockroach/blob/45deb66abbca3aae56bd27910a36d90a6a8bcafe/pkg/sql/vtable/pg_catalog.go#L608
            .add("proacl", STRING_ARRAY, x -> null)
            .build();
    }

    public static final class Entry {

        private static final Set<String> SET_TYPES = Set.of(
            ArrayType.NAME, RowType.NAME);

        public static Entry of(Signature signature) {
            return new Entry(
                signature.getName(),
                signature
            );
        }

        final Signature signature;
        final FunctionName functionName;

        final int returnTypeId;
        final boolean returnSetType;

        private Entry(FunctionName functionName, Signature signature) {
            this.signature = signature;
            this.functionName = functionName;

            var returnTypeSignature = signature.getReturnType();
            this.returnTypeId = pgTypeIdFrom(returnTypeSignature);
            this.returnSetType = SET_TYPES.contains(returnTypeSignature.getBaseTypeName());
        }

        static int pgTypeIdFrom(TypeSignature typeSignature) {
            var crateDataType = DataTypes.ofNameOrNull(typeSignature.getBaseTypeName());
            if (crateDataType == null) {
                if (ArrayType.NAME.equalsIgnoreCase(typeSignature.getBaseTypeName())) {
                    return PGArray.ANY_ARRAY.oid();
                } else {
                    return AnyType.INSTANCE.oid();
                }
            } else {
                return PGTypes.get(crateDataType).oid();
            }
        }
    }

    private static String prokind(Signature signature) {
        if (signature.getKind() == WINDOW) {
            return "w";
        } else if (signature.getKind() == AGGREGATE) {
            return "a";
        } else return "f";
    }
}
