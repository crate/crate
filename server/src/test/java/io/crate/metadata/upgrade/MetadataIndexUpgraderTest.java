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

package io.crate.metadata.upgrade;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.Constants;
import io.crate.metadata.RelationName;

public class MetadataIndexUpgraderTest extends ESTestCase {

    @Test
    public void testDynamicStringTemplateIsPurged() throws IOException {
        MetadataIndexUpgrader metadataIndexUpgrader = new MetadataIndexUpgrader();
        MappingMetadata mappingMetadata = new MappingMetadata(createDynamicStringMappingTemplate());
        MappingMetadata newMappingMetadata = metadataIndexUpgrader.createUpdatedIndexMetadata(mappingMetadata, "dummy", null);

        Object dynamicTemplates = newMappingMetadata.sourceAsMap().get("dynamic_templates");
        assertThat(dynamicTemplates, nullValue());

        // Check that the new metadata still has the root "default" element
        assertThat("{\"default\":{}}", is(newMappingMetadata.source().toString()));
    }

    @Test
    public void test__all_is_removed_from_mapping() throws Throwable {
        IndexMetadata indexMetadata = IndexMetadata.builder(new RelationName("doc", "users").indexNameOrAlias())
            .settings(Settings.builder().put("index.version.created", VersionUtils.randomVersion(random())))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(
                Constants.DEFAULT_MAPPING_TYPE,
                "{" +
                "   \"_all\": {\"enabled\": false}," +
                "   \"properties\": {" +
                "       \"name\": {" +
                "           \"type\": \"keyword\"" +
                "       }" +
                "   }" +
                "}")
            .build();

        MetadataIndexUpgrader metadataIndexUpgrader = new MetadataIndexUpgrader();
        IndexMetadata updatedMetadata = metadataIndexUpgrader.apply(indexMetadata, null);

        MappingMetadata mapping = updatedMetadata.mapping();
        assertThat(mapping.source().string(), Matchers.is("{\"default\":{\"properties\":{\"name\":{\"type\":\"keyword\",\"position\":1}}}}"));
    }

    @Test
    public void test_mappingMetadata_set_to_null() throws Throwable {
        IndexMetadata indexMetadata = IndexMetadata.builder(new RelationName("blob", "b1").indexNameOrAlias())
            .settings(Settings.builder().put("index.version.created", Version.V_4_7_0))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(null) // here
            .build();

        MetadataIndexUpgrader metadataIndexUpgrader = new MetadataIndexUpgrader();
        IndexMetadata updatedMetadata = metadataIndexUpgrader.apply(indexMetadata, null);

        assertThat(updatedMetadata.mapping(), is(nullValue()));
    }

    private static CompressedXContent createDynamicStringMappingTemplate() throws IOException {
        // @formatter:off
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .startArray("dynamic_templates")
                    .startObject()
                        .startObject("strings")
                            .field("match_mapping_type", "string")
                            .startObject("mapping")
                                .field("type", "keyword")
                                .field("doc_values", true)
                                .field("store", false)
                            .endObject()
                        .endObject()
                    .endObject()
                .endArray()
            .endObject()
            .endObject();
        // @formatter:on

        return new CompressedXContent(BytesReference.bytes(builder));
    }
}
