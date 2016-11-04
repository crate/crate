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

package io.crate.integrationtests;


import com.google.common.base.Joiner;
import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLOperations;
import io.crate.testing.SQLResponse;
import io.crate.testing.SQLTransportExecutor;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 1, transportClientRatio = 0)
public class ReadOnlyNodeIntegrationTest extends SQLTransportIntegrationTest {

    private SQLTransportExecutor readOnlyExecutor;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

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
        if ((nodeOrdinal + 1) % 2 == 0) {
            builder.put(SQLOperations.NODE_READ_ONLY_SETTING.getKey(), true);
        }
        return builder.build();
    }

    @Before
    public void setUpTestData() throws Exception {
        executeWrite("create table write_test (id int primary key, name string) with (number_of_replicas=0)");
        executeWrite("create table write_test2 (id int, name string) with (number_of_replicas=0)");
        executeWrite("create blob table write_blob_test with (number_of_replicas=0)");
        executeWrite("create repository existing_repo TYPE \"fs\" with (location=?, compress=True)", new Object[]{folder});
        ensureYellow();
    }

    private SQLResponse executeWrite(String stmt, Object[] args) {
        if (readOnlyExecutor == null) {
            readOnlyExecutor = new SQLTransportExecutor(
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
        response = readOnlyExecutor.exec(stmt, args);
        return response;
    }

    private SQLResponse executeWrite(String stmt) {
        return executeWrite(stmt, null);
    }

    private void assertReadOnly(String stmt, Object[] args) throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Only read operations are allowed on this node");
        execute(stmt, args);
    }

    private void assertReadOnly(String stmt) throws Exception {
        assertReadOnly(stmt, null);
    }

    /**
     * ALLOWED STATEMENT TESTS
     **/

    @Test
    public void testAllowedSelectSys() throws Exception {
        execute("select name from sys.cluster");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testAllowedSelect() throws Exception {
        execute("select name from write_test");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testAllowedExplainSelect() throws Exception {
        execute("explain select name from write_test");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testAllowedExplainWhichIncludesNestedWrite() throws Exception {
        String copyFilePath = getClass().getResource("/essetup/data/copy").getPath();
        String uriPath = Joiner.on("/").join(copyFilePath, "test_copy_from.json");

        execute("explain copy write_test from ?", new Object[]{uriPath});
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testAllowedShowCreateTable() throws Exception {
        execute("show create table write_test");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testAllowedCopyTo() throws Exception {
        String uri = folder.getRoot().toURI().toString();
        SQLResponse response = execute("copy write_test to directory ?", new Object[]{uri});
        assertThat(response.rowCount(), greaterThanOrEqualTo(0L));
    }


    /**
     * FORBIDDEN STATEMENT TESTS
     **/

    @Test
    public void testForbiddenCreateTable() throws Exception {
        assertReadOnly("create table test (id int)");
    }

    @Test
    public void testForbiddenDropTable() throws Exception {
        assertReadOnly("drop table write_test");
    }

    @Test
    public void testForbiddenCreateBlobTable() throws Exception {
        assertReadOnly("create blob table blobs_test");
    }

    @Test
    public void testForbiddenDropBlobTable() throws Exception {
        assertReadOnly("drop blob table write_blob_test");
    }

    @Test
    public void testForbiddenCopyFrom() throws Exception {
        assertReadOnly("copy write_test from '/tmp/copy.json'");
    }

    @Test
    public void testForbiddenInsertFromValues() throws Exception {
        assertReadOnly("insert into write_test (id) values (1)");
    }

    @Test
    public void testForbiddenInsertFromValuesOnDuplicateKey() throws Exception {
        assertReadOnly("insert into write_test (id) values (1) on duplicate key update name = 'foo'");
    }

    @Test
    public void testForbiddenInsertFromQuery() throws Exception {
        assertReadOnly("insert into write_test2 (select * from write_test)");
    }

    @Test
    public void testForbiddenUpdate() throws Exception {
        assertReadOnly("update write_test set name = 'foo'");
    }

    @Test
    public void testForbiddenDelete() throws Exception {
        assertReadOnly("delete from write_test");
    }

    @Test
    public void testForbiddenAlterTableSetParameter() throws Exception {
        assertReadOnly("alter table write_test set (number_of_replicas=1)");
    }

    @Test
    public void testForbiddenAlterTableAddColumn() throws Exception {
        assertReadOnly("alter table write_test add column new_column_name string");
    }

    @Test
    public void testForbiddenSetGlobal() throws Exception {
        assertReadOnly("set global PERSISTENT stats.enabled = false");
    }

    @Test
    public void testForbiddenResetGlobal() throws Exception {
        assertReadOnly("reset global stats.enabled");
    }

    @Test
    public void testForbiddenRefresh() throws Exception {
        assertReadOnly("refresh table write_test");
    }

    @Test
    public void testForbiddenCreateRepository() throws Exception {
        assertReadOnly("create repository new_repo type fs with (location=?)", new Object[]{folder});
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
