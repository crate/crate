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

package io.crate.metadata.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import io.crate.Constants;
import io.crate.common.collections.MapBuilder;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class DDLClusterStateHelpersTest {

    @Test
    public void testMergeTemplateMapping() throws Exception {

        XContentBuilder oldMappingBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("foo")
            .field("merge-this-field", "foo")
            .field("position", 1)
            .endObject()
            .endObject()
            .startObject("_meta")
            .startObject("meta1")
            .field("field", "val1")
            .field("position", 2)
            .endObject()
            .endObject()
            .endObject();

        Map<String, Object> oldMapping =
            XContentHelper.convertToMap(BytesReference.bytes(oldMappingBuilder), true, XContentType.JSON).map();

        XContentBuilder newMappingBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("foo")
            .field("merge-this-field", "bar")
            .field("position", 1)
            .endObject()
            .endObject()
            .startObject("_meta")
            .startObject("meta1")
            .field("field", "val1")
            .field("position", 2)
            .endObject()
            .startObject("meta2")
            .field("field", "val2")
            .field("position", 3)
            .endObject()
            .endObject()
            .endObject();

        Map<String, Object> newMapping =
            XContentHelper.convertToMap(BytesReference.bytes(newMappingBuilder), true, XContentType.JSON).map();

        Map<String, Object> mapping = DDLClusterStateHelpers.mergeTemplateMapping(
            IndexTemplateMetadata.builder("foo")
                .patterns(List.of("*"))
                .putMapping(Constants.DEFAULT_MAPPING_TYPE,
                            Strings.toString(XContentFactory.jsonBuilder().map(
                            MapBuilder.<String, Object>newMapBuilder()
                                .put(Constants.DEFAULT_MAPPING_TYPE, oldMapping)
                                .map())))
                .build(),
            newMapping);
        assertThat(Strings.toString(XContentFactory.jsonBuilder().map(mapping))).isEqualTo(
            "{\"_meta\":{\"meta2\":{\"field\":\"val2\",\"position\":3},\"meta1\":{\"field\":\"val1\",\"position\":2}},\"properties\":{\"foo\":{\"merge-this-field\":\"bar\",\"position\":1}}}");
    }

}
