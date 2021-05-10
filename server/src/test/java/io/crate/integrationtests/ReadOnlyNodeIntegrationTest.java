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


import io.crate.action.sql.SQLOperations;
import io.crate.testing.SQLResponse;
import io.crate.testing.SQLTransportExecutor;
import io.crate.testing.UseRandomizedSchema;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThrows;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 1)
@UseRandomizedSchema(random = false)
public class ReadOnlyNodeIntegrationTest extends SQLIntegrationTestCase {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private SQLTransportExecutor writeExecutor;

    public ReadOnlyNodeIntegrationTest() {
        super(new SQLTransportExecutor(
            new SQLTransportExecutor.ClientProvider() {
                @Override
                public Client client() {
                    // make sure we use the read-only client
                    return internalCluster().client(internalCluster().getNodeNames()[1]);
                }

                @Override
                public String pgUrl() {
                    return null;
                }

                @Override
                public SQLOperations sqlOperations() {
                    return internalCluster().getInstance(SQLOperations.class, internalCluster().getNodeNames()[1]);
                }
            }
        ));
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder();
        builder.put(super.nodeSettings(nodeOrdinal));
        builder.put("path.repo", folder.getRoot().getParent());
        if ((nodeOrdinal + 1) % 2 == 0) {
            builder.put(SQLOperations.NODE_READ_ONLY_SETTING.getKey(), true);
        }
        return builder.build();
    }

    @Before
    public void setUpTestData() throws Exception {
        executeWrite(
            "create repository existing_repo TYPE \"fs\" with (location=?, compress=True)",
            new Object[] { folder.getRoot().getAbsolutePath() }
        );
    }

    private SQLResponse executeWrite(String stmt, Object[] args) {
        if (writeExecutor == null) {
            writeExecutor = new SQLTransportExecutor(
                new SQLTransportExecutor.ClientProvider() {
                    @Override
                    public Client client() {
                        // make sure we use NOT the read-only client
                        return internalCluster().client(internalCluster().getNodeNames()[0]);
                    }

                    @Nullable
                    @Override
                    public String pgUrl() {
                        return null;
                    }

                    @Override
                    public SQLOperations sqlOperations() {
                        // make sure we use NOT the read-only operations
                        return internalCluster().getInstance(SQLOperations.class, internalCluster().getNodeNames()[0]);
                    }
                }
            );
        }
        response = writeExecutor.exec(stmt, args);
        return response;
    }

    private void assertReadOnly(String stmt, Object[] args) throws Exception {
        assertThrows(() -> execute(stmt, args),
                     isSQLError(containsString("Only read operations allowed on this node"),
                                INTERNAL_ERROR,
                                FORBIDDEN,
                                4031));
    }

    private void assertReadOnly(String stmt) throws Exception {
        assertReadOnly(stmt, null);
    }

    @Test
    public void testAllowedSelectSys() throws Exception {
        execute("select name from sys.cluster");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testForbiddenCreateTable() throws Exception {
        assertReadOnly("create table test (id int)");
    }


    @Test
    public void testForbiddenDropRepository() throws Exception {
        assertReadOnly("drop repository existing_repo");
    }

    @Test
    public void testForbiddenCreateSnapshot() throws Exception {
        assertReadOnly("create snapshot existing_repo.my_snap1 all");
    }

    @Test
    public void testForbiddenDropSnapshot() throws Exception {
        assertReadOnly("drop snapshot existing_repo.my_snap");
    }

    @Test
    public void testForbiddenRestore() throws Exception {
        assertReadOnly("restore snapshot existing_repo.my_snap all");
    }
}
