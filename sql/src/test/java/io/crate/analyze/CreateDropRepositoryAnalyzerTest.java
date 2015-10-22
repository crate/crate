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

import io.crate.exceptions.RepositoryUnknownException;
import io.crate.exceptions.RepositoryAlreadyExistsException;
import io.crate.metadata.MetaDataModule;
import io.crate.testing.MockedClusterServiceModule;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

public class CreateDropRepositoryAnalyzerTest extends BaseAnalyzerTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Mock
    private RepositoriesMetaData repositoriesMetaData;


    private class MyMockedClusterServiceModule extends MockedClusterServiceModule {

        @Override
        protected void configureMetaData(MetaData metaData) {
            when(metaData.custom(RepositoriesMetaData.TYPE)).thenReturn(repositoriesMetaData);
        }

    }

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                        new MyMockedClusterServiceModule(),
                        new MetaDataModule())
        );
        return modules;
    }

    @Before
    public void before() throws Exception {
        RepositoryMetaData repositoryMetaData = new RepositoryMetaData("my_repo", "fs", ImmutableSettings.EMPTY);
        when(repositoriesMetaData.repository(anyString())).thenReturn(null);
        when(repositoriesMetaData.repository("my_repo")).thenReturn(repositoryMetaData);
    }

    @Test
    public void testCreateRepository() throws Exception {
        CreateRepositoryAnalyzedStatement statement = (CreateRepositoryAnalyzedStatement)analyze("CREATE REPOSITORY \"new_repository\" TYPE \"fs\" with (location='/mount/backups/my_backup', compress=True)");
        assertThat(statement.repositoryName(), is("new_repository"));
        assertThat(statement.repositoryType(), is("fs"));
        assertThat(statement.settings().get("compress"), is("true"));
        assertThat(statement.settings().get("location"), is("/mount/backups/my_backup"));
    }

    @Test
    public void testCreateExistingRepository() throws Exception {
        expectedException.expect(RepositoryAlreadyExistsException.class);
        expectedException.expectMessage("Repository 'my_repo' already exists");
        analyze("create repository my_repo TYPE fs");
    }

    @Test
    public void testDropUnknownRepository() throws Exception {
        expectedException.expect(RepositoryUnknownException.class);
        expectedException.expectMessage("Repository 'unknown_repo' unknown");
        analyze("DROP REPOSITORY \"unknown_repo\"");
    }

    @Test
    public void testDropExistingRepo() throws Exception {
        DropRepositoryAnalyzedStatement statement = (DropRepositoryAnalyzedStatement)analyze("DROP REPOSITORY my_repo");
        assertThat(statement.repositoryName(), is("my_repo"));

    }
}
