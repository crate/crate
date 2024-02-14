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

package io.crate.fdw;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.expression.InputFactory;
import io.crate.fdw.ServersMetadata.Server;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.role.metadata.RolesHelper;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;

public class JdbcForeignDataWrapperTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_cannot_access_localhost_as_regular_user() throws Exception {
        Functions functions = new Functions(Map.of());
        Role arthur = RolesHelper.userOf("arthur");
        Roles roles = () -> List.of(arthur);
        NodeContext nodeCtx = new NodeContext(functions, roles);
        var fdw = new JdbcForeignDataWrapper(Settings.EMPTY, new InputFactory(nodeCtx));
        Map<String, Object> options = Map.of("url", "jdbc:postgresql://localhost:5432");
        Server server = new ServersMetadata.Server("self", "jdbc", "crate", Map.of(), options);
        CoordinatorTxnCtx txnCtx = CoordinatorTxnCtx.systemTransactionContext();
        RelationName relationName = new RelationName("secret", "documents");
        Reference nameRef = new SimpleReference(
            new ReferenceIdent(relationName, "name"),
            RowGranularity.DOC,
            DataTypes.STRING,
            1,
            null
        );
        assertThatThrownBy(() -> fdw.getIterator(arthur, server, txnCtx, relationName, List.of(nameRef)))
            .hasMessage("Only a super user can connect to localhost unless `fdw.allow_local` is set to true");
    }
}

