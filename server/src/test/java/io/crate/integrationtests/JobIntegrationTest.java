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

package io.crate.integrationtests;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import org.jetbrains.annotations.Nullable;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.session.Sessions;
import io.crate.exceptions.VersioningValidationException;
import io.crate.testing.Asserts;
import io.crate.testing.SQLTransportExecutor;

@IntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 1, supportsDedicatedMasters = false)
public class JobIntegrationTest extends IntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .build();
    }

    public JobIntegrationTest() {
        // ensure that the client node is used as handler and has no collectphase
        super(new SQLTransportExecutor(
            new SQLTransportExecutor.ClientProvider() {
                @Override
                public Client client() {
                    return cluster().coordOnlyNodeClient();
                }

                @Nullable
                @Override
                public String pgUrl() {
                    return null;
                }

                @Override
                public Sessions sqlOperations() {
                    return cluster().getInstance(Sessions.class);
                }
            }
        ));
    }

    @Test
    public void testFailurePropagationNonLocalCollectPhase() throws Exception {
        execute("create table users (name string) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();

        Asserts.assertSQLError(() -> execute("insert into users (name) (select name from users where _version = 1)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining(VersioningValidationException.VERSION_COLUMN_USAGE_MSG);

        Asserts.assertSQLError(() -> execute("select name from users where _version = 1"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining(VersioningValidationException.VERSION_COLUMN_USAGE_MSG);
    }
}
