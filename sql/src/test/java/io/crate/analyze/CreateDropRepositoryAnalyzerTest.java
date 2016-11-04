/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSortedMap;
import io.crate.exceptions.RepositoryAlreadyExistsException;
import io.crate.exceptions.RepositoryUnknownException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class CreateDropRepositoryAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        RepositoriesMetaData repositoriesMetaData = new RepositoriesMetaData(
            new RepositoryMetaData(
                "my_repo",
                "fs",
                Settings.builder().put("location", "/tmp/my_repo").build()
            ));
        ClusterState clusterState = ClusterState.builder(new ClusterName("testing"))
            .metaData(MetaData.builder()
                .putCustom(RepositoriesMetaData.TYPE, repositoriesMetaData))
            .build();
        ClusterServiceUtils.setState(clusterService, clusterState);
        e = SQLExecutor.builder(clusterService).build();
    }

    @Test
    public void testCreateRepository() throws Exception {
        CreateRepositoryAnalyzedStatement statement = e.analyze(
            "CREATE REPOSITORY \"new_repository\" TYPE \"fs\" with (location='/mount/backups/my_backup', compress=True)");
        assertThat(statement.repositoryName(), is("new_repository"));
        assertThat(statement.repositoryType(), is("fs"));
        assertThat(statement.settings().get("compress"), is("true"));
        assertThat(statement.settings().get("location"), is("/mount/backups/my_backup"));
    }

    @Test
    public void testCreateExistingRepository() throws Exception {
        expectedException.expect(RepositoryAlreadyExistsException.class);
        expectedException.expectMessage("Repository 'my_repo' already exists");
        e.analyze("create repository my_repo TYPE fs");
    }

    @Test
    public void testDropUnknownRepository() throws Exception {
        expectedException.expect(RepositoryUnknownException.class);
        expectedException.expectMessage("Repository 'unknown_repo' unknown");
        e.analyze("DROP REPOSITORY \"unknown_repo\"");
    }

    @Test
    public void testDropExistingRepo() throws Exception {
        DropRepositoryAnalyzedStatement statement = e.analyze("DROP REPOSITORY my_repo");
        assertThat(statement.repositoryName(), is("my_repo"));

    }

    @Test
    public void testCreateS3RepositoryWithAllSettings() throws Exception {
        CreateRepositoryAnalyzedStatement analysis = e.analyze("CREATE REPOSITORY foo TYPE s3 WITH (" +
                                                             "bucket='abc'," +
                                                             "region='us-north-42'," +
                                                             "endpoint='www.example.com'," +
                                                             "protocol='arp'," +
                                                             "base_path='/holz/'," +
                                                             "access_key='0xAFFE'," +
                                                             "secret_key='0xCAFEE'," +
                                                             "concurrent_streams=4," +
                                                             "chunk_size='12mb'," +
                                                             "compress=true," +
                                                             "server_side_encryption=false," +
                                                             "buffer_size='128k'," +
                                                             "max_retries=2," +
                                                             "canned_acl=false)");
        assertThat(analysis.repositoryType(), is("s3"));
        assertThat(analysis.repositoryName(), is("foo"));
        Map<String, String> sortedSettingsMap = ImmutableSortedMap.copyOf(analysis.settings().getAsMap());
        assertThat(
            Joiner.on(",").withKeyValueSeparator(":")
                .join(sortedSettingsMap),
            is("access_key:0xAFFE," +
               "base_path:/holz/," +
               "bucket:abc," +
               "buffer_size:131072b," +
               "canned_acl:false," +
               "chunk_size:12582912b," +
               "compress:true," +
               "concurrent_streams:4," +
               "endpoint:www.example.com," +
               "max_retries:2," +
               "protocol:arp," +
               "region:us-north-42," +
               "secret_key:0xCAFEE," +
               "server_side_encryption:false"));
    }

    @Test
    public void testCreateS3RepoWithoutSettings() throws Exception {
        CreateRepositoryAnalyzedStatement analysis = e.analyze("CREATE REPOSITORY foo TYPE s3");
        assertThat(analysis.repositoryName(), is("foo"));
        assertThat(analysis.repositoryType(), is("s3"));
        // two settings are there because those have default values
        assertThat(analysis.settings().getAsMap().keySet(), containsInAnyOrder("compress", "server_side_encryption"));
    }

    @Test
    public void testCreateS3RepoWithWrongSettings() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("setting 'wrong' not supported");
        e.analyze("CREATE REPOSITORY foo TYPE s3 WITH (wrong=true)");
    }
}
