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

package io.crate.plugin.aws.s3;


import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSortedMap;
import io.crate.action.sql.SQLBulkRequest;
import io.crate.action.sql.SQLRequest;
import io.crate.analyze.Analyzer;
import io.crate.analyze.CreateRepositoryAnalyzedStatement;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.repositories.RepositorySettingsModule;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.Schemas;
import io.crate.plugin.aws.S3RepositoryAnalysisModule;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.common.inject.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3RepositoryAnalyzerTest extends CrateUnitTest {

    @Mock
    private RepositoriesMetaData repositoriesMetaData;

    @Mock
    private ThreadPool threadPool;


    private Analyzer analyzer;

    private class MyMockedClusterServiceModule extends AbstractModule {

        @Override
        protected void configure() {
            ClusterService clusterService = mock(ClusterService.class);
            ClusterState state = mock(ClusterState.class);
            MetaData metaData = mock(MetaData.class);
            when(metaData.settings()).thenReturn(Settings.EMPTY);
            when(state.metaData()).thenReturn(metaData);
            when(clusterService.state()).thenReturn(state);
            bind(ClusterService.class).toInstance(clusterService);
            bind(Settings.class).toInstance(Settings.EMPTY);

            bind(TransportActionProvider.class).toInstance(mock(TransportActionProvider.class));

            bind(TransportPutIndexTemplateAction.class).toInstance(mock(TransportPutIndexTemplateAction.class));

            when(metaData.custom(RepositoriesMetaData.TYPE)).thenReturn(repositoriesMetaData);
        }
    }

    @Before
    public void prepareModules() throws Exception {
        ModulesBuilder builder = new ModulesBuilder();
        builder.add(new Module() {
            @Override
            public void configure(Binder binder) {
                binder.bind(ThreadPool.class).toInstance(threadPool);
            }
        });
        builder.add(
                new RepositorySettingsModule(),
                new S3RepositoryAnalysisModule(Settings.EMPTY),
                new MyMockedClusterServiceModule(),
                new MetaDataModule()
        );
        Injector injector = builder.createInjector();
        analyzer = injector.getInstance(Analyzer.class);
    }

    private CreateRepositoryAnalyzedStatement analyze(String statement) {
        return (CreateRepositoryAnalyzedStatement)analyzer.analyze(SqlParser.createStatement(statement),
                new ParameterContext(SQLRequest.EMPTY_ARGS, SQLBulkRequest.EMPTY_BULK_ARGS, Schemas.DEFAULT_SCHEMA_NAME)).analyzedStatement();
    }

    @Test
    public void testCreateS3RepositoryWithAllSettings() throws Exception {
        CreateRepositoryAnalyzedStatement analysis = analyze("CREATE REPOSITORY foo TYPE s3 WITH (" +
                                                             "bucket='abc'," +
                                                             "region='us-north-42'," +
                                                             "endpoint='www.example.com'," +
                                                             "protocol='arp'," +
                                                             "base_path='/holz/'," +
                                                             "access_key='0xAFFE'," +
                                                             "secret_key='0xCAFEE'," +
                                                             "concurrent_streams=4," +
                                                             "chunk_size=12," +
                                                             "compress=true," +
                                                             "server_side_encryption='only for pussies'," +
                                                             "buffer_size=128," +
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
                   "buffer_size:128," +
                   "canned_acl:false," +
                   "chunk_size:12," +
                   "compress:true," +
                   "concurrent_streams:4," +
                   "endpoint:www.example.com," +
                   "max_retries:2," +
                   "protocol:arp," +
                   "region:us-north-42," +
                   "secret_key:0xCAFEE," +
                   "server_side_encryption:only for pussies"));
    }

    @Test
    public void testWithoutSettings() throws Exception {
        CreateRepositoryAnalyzedStatement analysis = analyze("CREATE REPOSITORY foo TYPE s3");
        assertThat(analysis.repositoryName(), is("foo"));
        assertThat(analysis.repositoryType(), is("s3"));
        assertThat(analysis.settings().getAsMap().isEmpty(), is(true));
    }

    @Test
    public void testWrongSettings() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid parameters specified: [wrong]");
        analyze("CREATE REPOSITORY foo TYPE s3 WITH (wrong=true)");

    }
}

