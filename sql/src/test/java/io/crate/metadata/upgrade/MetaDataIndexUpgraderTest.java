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

package io.crate.metadata.upgrade;

import io.crate.Constants;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.nullValue;

public class MetaDataIndexUpgraderTest extends CrateUnitTest {

    @Test
    public void testDynamicStringTemplateIsPurged() throws IOException {
        MetaDataIndexUpgrader metaDataIndexUpgrader = new MetaDataIndexUpgrader();
        MappingMetaData mappingMetaData = new MappingMetaData(createDynamicStringMappingTemplate());
        MappingMetaData newMappingMetaData = metaDataIndexUpgrader.purgeDynamicStringTemplate(mappingMetaData, "dummy");

        Object dynamicTemplates = newMappingMetaData.getSourceAsMap().get("dynamic_templates");
        assertThat(dynamicTemplates, nullValue());

        // Check that the new metadata still has the root "default" element
        assertThat("{\"default\":{}}", is(newMappingMetaData.source().toString()));
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
