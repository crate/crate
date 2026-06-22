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

import java.util.stream.Collectors;

import io.crate.data.Input;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.OidHash;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.pgcatalog.PgCatalogTableDefinitions;
import io.crate.metadata.table.ConstraintInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.role.Securable;
import io.crate.sql.tree.CheckConstraint;
import io.crate.types.DataTypes;

/**
 * Implements {@code pg_get_constraintdef(oid [, pretty])}, returning the SQL definition of the
 * constraint with the given {@code pg_constraint} OID, e.g. {@code PRIMARY KEY (id)} or
 * {@code CHECK (...)}. Used by ORM introspection (Prisma, TypeORM; see issue #18795).
 *
 * <p>There is no reverse OID index, so the OID is matched by recomputing
 * {@link OidHash#constraintOid} per constraint, using the constraint type's <em>text</em> form</p>
 *
 * <p>Results are limited to constraints whose table the session user can see, mirroring the row
 * filter applied to {@code pg_constraint}. PostgreSQL itself performs no privilege check here
 * (its system catalogs are world-readable); restricting to visible tables is a deliberate
 * CrateDB choice that keeps this function consistent with our filtered {@code pg_constraint}.</p>
 */
public class PgGetConstraintDefFunction extends Scalar<String, Object> {

    public static final String NAME = "pg_get_constraintdef";
    private static final FunctionName FQN = new FunctionName(PgCatalogSchemaInfo.NAME, NAME);

    public static void register(Functions.Builder module) {
        module.add(
            Signature.builder(FQN, FunctionType.SCALAR)
                .argumentTypes(DataTypes.INTEGER.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            PgGetConstraintDefFunction::new
        );
        module.add(
            Signature.builder(FQN, FunctionType.SCALAR)
                .argumentTypes(DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.BOOLEAN.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            PgGetConstraintDefFunction::new
        );
    }

    public PgGetConstraintDefFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    @SafeVarargs
    public final String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>... args) {
        Integer oid = (Integer) args[0].value();
        if (oid == null) {
            return null;
        }
        if (args.length == 2) {
            Object pretty = args[1].value();
            if (pretty == null) {
                return null;
            }
        }
        Roles roles = nodeCtx.roles();
        Role user = roles.findUser(txnCtx.sessionSettings().userName());
        if (user == null) {
            return null;
        }
        for (SchemaInfo schema : nodeCtx.schemas()) {
            boolean visibleToAll = PgCatalogTableDefinitions.isPgCatalogOrInformationSchema(schema.name());
            for (TableInfo table : schema.getTables()) {
                String pkName = table.pkConstraintNameOrDefault();
                var checkConstraints = table.checkConstraints();
                if (pkName == null && checkConstraints.isEmpty()) {
                    continue;
                }
                String fqn = table.ident().fqn();
                if (!visibleToAll && !roles.hasAnyPrivilege(user, Securable.TABLE, fqn)) {
                    continue;
                }
                if (pkName != null
                        && OidHash.constraintOid(fqn, pkName, ConstraintInfo.Type.PRIMARY_KEY.toString()) == oid) {
                    return renderPrimaryKey(table);
                }
                for (CheckConstraint<?> check : checkConstraints) {
                    if (OidHash.constraintOid(fqn, check.name(), ConstraintInfo.Type.CHECK.toString()) == oid) {
                        return "CHECK (" + check.expressionStr() + ")";
                    }
                }
            }
        }
        return null;
    }

    private static String renderPrimaryKey(TableInfo table) {
        String columns = table.primaryKey().stream()
            .map(ColumnIdent::quotedOutputName)
            .collect(Collectors.joining(", "));
        return "PRIMARY KEY (" + columns + ")";
    }
}
