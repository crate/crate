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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.fs.FsRepository;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.hamcrest.Matchers.is;

public class RepositoryIntegrationTest extends SQLTransportIntegrationTest {

    private static final String REPO_NAME = "existing_repo";

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

    @Before
    public void prepare() throws Exception {
        client().admin().cluster()
                .preparePutRepository(REPO_NAME)
                .setType(FsRepository.TYPE)
                .setSettings(ImmutableSettings.builder()
                        .put("location", new File(temporaryFolder.getRoot(), REPO_NAME)))
                .execute().actionGet(TimeValue.timeValueSeconds(2));
        waitNoPendingTasksOnAll();
    }

    @After
    public void cleanUp() throws Exception {
        try {
            client().admin().cluster().prepareDeleteRepository(REPO_NAME)
                    .execute().actionGet(TimeValue.timeValueSeconds(2));
        } catch (RepositoryMissingException e) {
            // ignore
        }
    }

    @Test
    public void testDropExistingRepository() throws Exception {
        execute("DROP REPOSITORY " + REPO_NAME);
        assertThat(response.rowCount(), is(1L));

        waitNoPendingTasksOnAll();

        expectedException.expect(RepositoryMissingException.class);
        expectedException.expectMessage("[existing_repo] missing");
        client().admin().cluster()
                .prepareGetRepositories(REPO_NAME)
                .execute().actionGet(TimeValue.timeValueSeconds(2));
    }

    @Test
    public void testDropNonExistingRepository() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Repository 'does_not_exist' unknown");
        execute("DROP REPOSITORY does_not_exist");
    }
}
