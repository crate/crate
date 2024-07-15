/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.expression.scalar.postgres;

import static io.crate.role.Permission.READ_WRITE_DEFINE;

import io.crate.data.Input;
import io.crate.exceptions.RelationUnknown;
import io.crate.expression.scalar.HasTablePrivilegeFunction;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.Scalar;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.settings.SessionSettings;
import io.crate.metadata.table.Operation;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataTypes;

public class PgTableIsVisibleFunction extends Scalar<Boolean, Integer> {

    private static final FunctionName NAME = new FunctionName(PgCatalogSchemaInfo.NAME, "pg_table_is_visible");

    public static void register(Functions.Builder module) {
        module.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.INTEGER.getTypeSignature())
                .returnType(DataTypes.BOOLEAN.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            PgTableIsVisibleFunction::new);
    }

    protected PgTableIsVisibleFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @SafeVarargs
    @Override
    public final Boolean evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<Integer>... args) {
        assert args.length == 1 : NAME + " expects exactly 1 argument, got " + args.length;
        Integer tableOid = args[0].value();
        if (tableOid == null) {
            return null;
        }

        Schemas schemas = nodeContext.schemas();
        RelationName relationName = schemas.getRelation(tableOid);
        if (relationName == null) {
            return false;
        }

        SessionSettings sessionSettings = txnCtx.sessionSettings();
        Roles roles = nodeContext.roles();
        Role user = roles.findUser(sessionSettings.userName());
        for (String searchPathIdx : sessionSettings.searchPath()) {
            try {
                schemas.findRelation(
                    QualifiedName.of(searchPathIdx, relationName.name()),
                    Operation.READ,
                    user,
                    SearchPath.createSearchPathFrom(searchPathIdx));
            } catch (RelationUnknown e) {
                continue;
            }
            boolean foundMatchingSchemaName = searchPathIdx.equals(relationName.schema());
            if (HasTablePrivilegeFunction.checkByTableName(
                roles,
                user,
                new RelationName(searchPathIdx, relationName.name()).fqn(),
                READ_WRITE_DEFINE,
                schemas)) {
                // return true if the found relation is from the correct schema or return false.
                return foundMatchingSchemaName;
            }
            // do not iterate further if the matching schema is found from searchPaths
            if (foundMatchingSchemaName) {
                break;
            }
        }
        return false;
    }
}
