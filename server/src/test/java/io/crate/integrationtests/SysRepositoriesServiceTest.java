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

import io.crate.testing.UseJdbc;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.StringType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope()
@UseJdbc(0) // missing column types
public class SysRepositoriesServiceTest extends SQLIntegrationTestCase {

    @ClassRule
    public static TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    private List<String> repositories = new ArrayList<>();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("path.repo", TEMP_FOLDER.getRoot().getAbsolutePath())
            .build();
    }

    @Before
    public void setUpRepositories() throws Exception {
        createRepository("test-repo");
    }

    @After
    public void cleanUp() throws Exception {
        Iterator<String> it = repositories.iterator();
        while (it.hasNext()) {
            deleteRepository(it.next());
            it.remove();
        }
    }

    private void createRepository(String name) {
        AcknowledgedResponse putRepositoryResponse = client().admin().cluster().preparePutRepository(name)
            .setType("fs")
            .setSettings(Settings.builder()
                .put("location", new File(TEMP_FOLDER.getRoot(), "backup").getAbsolutePath())
                .put("chunk_size", "5k")
                .put("compress", false)
            ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
        repositories.add(name);
    }

    private void deleteRepository(String name) {
        AcknowledgedResponse deleteRepositoryResponse = client().admin().cluster().prepareDeleteRepository(name).get();
        assertThat(deleteRepositoryResponse.isAcknowledged(), equalTo(true));
    }

    @Test
    public void testQueryAllColumns() throws Exception {
        execute("select * from sys.repositories");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols().length, is(3));
        assertThat(response.cols(), is(new String[]{"name", "settings", "type"}));
        assertThat(response.columnTypes(), is(new DataType[]{StringType.INSTANCE, DataTypes.UNTYPED_OBJECT, StringType.INSTANCE}));
        assertThat((String) response.rows()[0][0], is("test-repo"));

        Map<String, Object> settings = (Map<String, Object>) response.rows()[0][1];
        assertThat(settings.size(), is(3));
        assertThat((String) settings.get("location"), is(new File(TEMP_FOLDER.getRoot(), "backup").getAbsolutePath()));
        assertThat((String) settings.get("chunk_size"), is("5k"));
        assertThat((String) settings.get("compress"), is("false"));

        assertThat((String) response.rows()[0][2], is("fs"));
    }
}
