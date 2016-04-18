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

package io.crate.metadata;

import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReferenceInfosTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    public ClusterService clusterService;

    @Mock
    public ClusterState clusterState;

    @Mock
    public MetaData metaData;

    @Mock
    public Functions functions;

    @Mock
    public Provider<TransportPutIndexTemplateAction> transportPutIndexTemplateActionProvider;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metaData()).thenReturn(metaData);
        when(metaData.concreteAllOpenIndices()).thenReturn(new String[0]);
        when(metaData.templates()).thenReturn(ImmutableOpenMap.<String, IndexTemplateMetaData>of());
    }

    @Test
    public void testSystemSchemaIsNotWritable() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("The table foo.bar is read-only. Write, Drop or Alter operations are not supported");

        TableIdent tableIdent = new TableIdent("foo", "bar");
        SchemaInfo schemaInfo = mock(SchemaInfo.class);
        TableInfo tableInfo = mock(TableInfo.class);
        when(tableInfo.ident()).thenReturn(tableIdent);
        when(schemaInfo.getTableInfo(tableIdent.name())).thenReturn(tableInfo);
        when(schemaInfo.name()).thenReturn(tableIdent.schema());
        when(schemaInfo.systemSchema()).thenReturn(true);

        Schemas schemas = getReferenceInfos(schemaInfo);
        schemas.getWritableTable(tableIdent);
    }

    @Test
    public void testTableAliasIsNotWritable() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("foo.bar is an alias. Write, Drop or Alter operations are not supported");

        TableIdent tableIdent = new TableIdent("foo", "bar");
        SchemaInfo schemaInfo = mock(SchemaInfo.class);
        DocTableInfo tableInfo = mock(DocTableInfo.class);
        when(tableInfo.ident()).thenReturn(tableIdent);
        when(schemaInfo.getTableInfo(tableIdent.name())).thenReturn(tableInfo);
        when(schemaInfo.name()).thenReturn(tableIdent.schema());
        when(tableInfo.isAlias()).thenReturn(true);

        when(schemaInfo.systemSchema()).thenReturn(false);

        Schemas schemas = getReferenceInfos(schemaInfo);
        schemas.getWritableTable(tableIdent);
    }

    private Schemas getReferenceInfos(SchemaInfo schemaInfo) {
        Map<String, SchemaInfo> builtInSchema = new HashMap<>();
        builtInSchema.put(schemaInfo.name(), schemaInfo);

        return new ReferenceInfos(
                builtInSchema,
                clusterService,
                new IndexNameExpressionResolver(Settings.EMPTY),
                mock(ThreadPool.class),
                transportPutIndexTemplateActionProvider,
                functions);
    }
}