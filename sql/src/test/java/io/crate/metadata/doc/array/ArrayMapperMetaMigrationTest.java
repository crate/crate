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

package io.crate.metadata.doc.array;

import io.crate.Constants;
import io.crate.metadata.ColumnIdent;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;

public class ArrayMapperMetaMigrationTest extends CrateUnitTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private InternalNode startNode(File dataFolder) {

        InternalNode node = (InternalNode) NodeBuilder.nodeBuilder().local(true).data(true).settings(
                ImmutableSettings.builder()
                        .put("path.data", dataFolder.getAbsolutePath())
                        .put(ClusterName.SETTING, getClass().getName())
                        .put("node.name", getClass().getName())
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("config.ignore_system_properties", true)
                        .put("gateway.type", "local")).build();
        node.start();
        return node;
    }

    @Test
    public void testMigrateOldIndices() throws Exception {
        File dataFolder = tempFolder.newFolder();
        InternalNode node = startNode(dataFolder);
        node.client().admin().indices().prepareCreate("with_meta")
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, XContentFactory.jsonBuilder()
                                .startObject()
                                .startObject("_meta")
                                .startObject("columns")
                                .startObject("a")
                                .field("collection_type", "array")
                                .endObject()
                                .endObject()
                                .endObject()
                                .startObject("properties")
                                .startObject("a")
                                .field("type", "string")
                                .field("index", "not_analyzed")
                                .endObject()
                                .endObject()
                                .endObject()
                ).addMapping("other", XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("_meta")
                        .startObject("columns")
                        .startObject("a")
                        .field("collection_type", "array")
                        .endObject()
                        .endObject()
                        .endObject()
                        .startObject("properties")
                        .startObject("a")
                        .field("type", "string")
                        .field("index", "not_analyzed")
                        .endObject()
                        .endObject()
                        .endObject()
        ).execute().actionGet();
        node.client().admin().indices().prepareCreate("without_meta")
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, XContentFactory.jsonBuilder()
                                .startObject()
                                .startObject("properties")
                                .startObject("a")
                                .field("type", "string")
                                .field("index", "not_analyzed")
                                .endObject()
                                .endObject()
                                .endObject()
                ).execute().actionGet();
        node.client().admin().cluster().prepareHealth()
                .setWaitForRelocatingShards(0).setWaitForGreenStatus()
                .execute().actionGet();
        // restart the node
        node.stop();
        node.close();
        node = null;

        InternalNode newNode = startNode(dataFolder);

        newNode.client().admin().cluster().prepareHealth()
                .setWaitForRelocatingShards(0).setWaitForGreenStatus()
                .execute().actionGet();
        MetaData metaData = newNode.client().admin().cluster().prepareState()
                .execute().actionGet().getState().metaData();
        IndexMetaData withMeta = metaData.index("with_meta");

        // _meta.columns is gone
        assertThat(withMeta.mapping(Constants.DEFAULT_MAPPING_TYPE).source().string(),
                is("{\"default\":{\"properties\":{\"a\":{\"type\":\"array\",\"inner\":{\"type\":\"string\",\"index\":\"not_analyzed\"}}}}}"));

        // _meta.columns still there
        assertThat(withMeta.mapping("other").source().string(),
                is("{\"other\":{\"_meta\":{\"columns\":{\"a\":{\"collection_type\":\"array\"}}},\"properties\":{\"a\":{\"type\":\"string\",\"index\":\"not_analyzed\"}}}}"));
        assertThat(withMeta.version(), is(greaterThan(1L)));
        IndexMetaData withOutMeta = newNode.client().admin().cluster().prepareState()
                .execute().actionGet().getState().metaData().index("without_meta");
        // no change
        assertThat(withOutMeta.mapping(Constants.DEFAULT_MAPPING_TYPE).source().string(), is("{\"default\":{\"properties\":{\"a\":{\"type\":\"string\",\"index\":\"not_analyzed\"}}}}"));
        assertThat(withOutMeta.version(), is(1L));
        newNode.stop();
        newNode.close();
    }

    @Test
    public void testGetByIdent() throws Exception {
        Map<String, Object> mapping = XContentHelper.convertToMap("{\"a\":{\"type\":\"object\", \"properties\":{\"b\":{\"type\":\"string\"}}}}".getBytes(), false).v2();
        Object gotMapping = ArrayMapperMetaMigration.getColumnMapping(mapping, ColumnIdent.fromPath("a.b"));
        assertThat(gotMapping, Matchers.is(notNullValue()));
        assertThat(gotMapping, instanceOf(Map.class));
        assertThat(XContentFactory.jsonBuilder().map((Map)gotMapping).string(), Matchers.is("{\"type\":\"string\"}"));
    }

    @Test
    public void testGetByIdentTopLevel() throws Exception {
        Map<String, Object> mapping = XContentHelper.convertToMap("{\"a\":{\"type\":\"object\", \"properties\":{\"b\":{\"type\":\"string\"}}}}".getBytes(), false).v2();
        Object gotMapping = ArrayMapperMetaMigration.getColumnMapping(mapping, ColumnIdent.fromPath("a"));
        assertThat(gotMapping, Matchers.is(notNullValue()));
        assertThat(gotMapping, instanceOf(Map.class));
        assertThat(XContentFactory.jsonBuilder().map((Map) gotMapping).string(), Matchers.is("{\"properties\":{\"b\":{\"type\":\"string\"}},\"type\":\"object\"}"));
    }

    @Test
    public void testGetByIdentUnknown() throws Exception {
        Map<String, Object> mapping = XContentHelper.convertToMap("{\"a\":{\"type\":\"object\", \"properties\":{\"b\":{\"type\":\"string\"}}}}".getBytes(), false).v2();
        Object gotMapping = ArrayMapperMetaMigration.getColumnMapping(mapping, ColumnIdent.fromPath("a.c"));
        assertThat(gotMapping, Matchers.is(nullValue()));
    }

    @Test
    public void testGetByIdentUnknownNested() throws Exception {
        Map<String, Object> mapping = XContentHelper.convertToMap("{\"a\":{\"type\":\"object\", \"properties\":{\"b\":{\"type\":\"string\"}}}}".getBytes(), false).v2();
        Object gotMapping = ArrayMapperMetaMigration.getColumnMapping(mapping, ColumnIdent.fromPath("a.b.c"));
        assertThat(gotMapping, Matchers.is(nullValue()));
    }

    @Test
    public void testGetByIdentUnknownTopLevel() throws Exception {
        Map<String, Object> mapping = XContentHelper.convertToMap("{\"a\":{\"type\":\"object\", \"properties\":{\"b\":{\"type\":\"string\"}}}}".getBytes(), false).v2();
        Object gotMapping = ArrayMapperMetaMigration.getColumnMapping(mapping, ColumnIdent.fromPath("b"));
        assertThat(gotMapping, Matchers.is(nullValue()));
    }

    @Test
    public void testPutByIdent() throws Exception {
        Map<String, Object> mapping = XContentHelper.convertToMap("{\"a\":{\"type\":\"object\", \"properties\":{\"b\":{\"type\":\"string\"}}}}".getBytes(), false).v2();
        ArrayMapperMetaMigration.overrideExistingColumnMapping(mapping, ColumnIdent.fromPath("a.b"), 1);
        assertThat(XContentFactory.jsonBuilder().map(mapping).string(), Matchers.is("{\"a\":{\"properties\":{\"b\":1},\"type\":\"object\"}}"));
    }

    @Test
    public void testPutByIdentNewUnchanged() throws Exception {
        Map<String, Object> mapping = XContentHelper.convertToMap("{\"a\":{\"type\":\"object\", \"properties\":{\"c\":{\"type\":\"string\"}}}}".getBytes(), false).v2();
        ArrayMapperMetaMigration.overrideExistingColumnMapping(mapping, ColumnIdent.fromPath("a.b"), 1);

        // unchanged
        assertThat(XContentFactory.jsonBuilder().map(mapping).string(), Matchers.is("{\"a\":{\"properties\":{\"c\":{\"type\":\"string\"}},\"type\":\"object\"}}"));
    }

    @Test
    public void testPutByIdentNewNestedUnchanged() throws Exception {
        Map<String, Object> mapping = XContentHelper.convertToMap("{\"a\":{\"type\":\"object\", \"properties\":{\"c\":{\"type\":\"string\"}}}}".getBytes(), false).v2();
        ArrayMapperMetaMigration.overrideExistingColumnMapping(mapping, ColumnIdent.fromPath("a.b.c"), 1);
        assertThat(XContentFactory.jsonBuilder().map(mapping).string(), Matchers.is("{\"a\":{\"properties\":{\"c\":{\"type\":\"string\"}},\"type\":\"object\"}}"));
    }

    @Test
    public void testPutTopLevel() throws Exception {
        Map<String, Object> mapping = XContentHelper.convertToMap("{\"a\":{\"type\":\"object\", \"properties\":{\"c\":{\"type\":\"string\"}}}}".getBytes(), false).v2();
        ArrayMapperMetaMigration.overrideExistingColumnMapping(mapping, ColumnIdent.fromPath("a"), 1);
        assertThat(XContentFactory.jsonBuilder().map(mapping).string(), Matchers.is("{\"a\":1}"));
    }

    @Test
    public void testPutTopLevelNewUnchanged() throws Exception {
        Map<String, Object> mapping = XContentHelper.convertToMap("{\"a\":{\"type\":\"object\", \"properties\":{\"c\":{\"type\":\"string\"}}}}".getBytes(), false).v2();
        ArrayMapperMetaMigration.overrideExistingColumnMapping(mapping, ColumnIdent.fromPath("b"), 1);
        assertThat(XContentFactory.jsonBuilder().map(mapping).string(), Matchers.is("{\"a\":{\"properties\":{\"c\":{\"type\":\"string\"}},\"type\":\"object\"}}"));
    }
}
