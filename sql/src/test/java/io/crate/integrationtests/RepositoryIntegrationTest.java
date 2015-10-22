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

import com.google.common.io.Files;
import io.crate.action.sql.SQLActionException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.HashMap;

import static org.hamcrest.Matchers.is;

public class RepositoryIntegrationTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final File TEMP_FOLDER_ROOT = Files.createTempDir();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(TEMP_FOLDER_ROOT);

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder().put(super.nodeSettings(nodeOrdinal))
                .put("path.repo", TEMP_FOLDER_ROOT.getAbsolutePath())
                .build();
    }

    @AfterClass
    public static void deleteTempFolderRoot() throws Exception {
        TEMP_FOLDER_ROOT.delete();
    }

    @Test
    public void testDropExistingRepository() throws Exception {
        execute("CREATE REPOSITORY existing_repo TYPE \"fs\" with (location=?, compress=True)",
                new Object[]{TEMP_FOLDER_ROOT.getAbsolutePath()});
        waitNoPendingTasksOnAll();
        execute("DROP REPOSITORY existing_repo");
        assertThat(response.rowCount(), is(1L));

        waitNoPendingTasksOnAll();
        execute("select * from sys.repositories where name = ? ", new Object[]{"existing_repo"});
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testDropNonExistingRepository() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Repository 'does_not_exist' unknown");
        execute("DROP REPOSITORY does_not_exist");
    }

    @Test
    public void testCreateRepository() throws Throwable {
        execute("CREATE REPOSITORY \"myRepo\" TYPE \"fs\" with (location=?, compress=True)",
                new Object[]{TEMP_FOLDER_ROOT.getAbsolutePath()});
        waitNoPendingTasksOnAll();
        execute("select * from sys.repositories where name ='myRepo'");
        assertThat(response.rowCount(), is(1L));
        assertThat((String) response.rows()[0][0], is("myRepo"));
        assertThat((String) response.rows()[0][1], is("fs"));
        HashMap<String, String> settings = (HashMap)response.rows()[0][2];
        assertThat(settings.get("compress"), is("true"));
        assertThat(new File(settings.get("location")).getParent(), is(TEMP_DIR.getAbsolutePath()));
    }

    @Test
    public void testCreateExistingRepository() throws Throwable {
        execute("CREATE REPOSITORY \"myRepo\" TYPE \"fs\" with (location=?, compress=True)",
                new Object[]{TEMP_FOLDER_ROOT.getAbsolutePath()});
        waitNoPendingTasksOnAll();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Repository 'myRepo' already exists");
        execute("CREATE REPOSITORY \"myRepo\" TYPE \"fs\" with (location=?, compress=True)",
                new Object[]{TEMP_FOLDER_ROOT.getAbsolutePath()});
    }
}
