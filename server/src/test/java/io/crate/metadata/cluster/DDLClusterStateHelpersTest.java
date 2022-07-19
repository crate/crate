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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Map;

import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import io.crate.Constants;
import io.crate.common.collections.MapBuilder;

public class DDLClusterStateHelpersTest {

    @Test
    public void testMergeTemplateMapping() throws Exception {
        Map<String, Object> oldMapping = MapBuilder.<String, Object>newMapBuilder()
            .put("properties", MapBuilder.<String, String>newMapBuilder().put("foo", "foo").map())
            .put("_meta", MapBuilder.<String, String>newMapBuilder().put("meta1", "val1").map())
            .map();

        Map<String, Object> newMapping = MapBuilder.<String, Object>newMapBuilder()
            .put("properties", MapBuilder.<String, String>newMapBuilder().put("foo", "bar").map())
            .put("_meta", MapBuilder.<String, String>newMapBuilder()
                .put("meta1", "v1")
                .put("meta2", "v2")
                .map())
            .map();

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
        assertThat(Strings.toString(XContentFactory.jsonBuilder().map(mapping)), is(
            "{\"_meta\":{\"meta2\":\"v2\",\"meta1\":\"v1\"},\"properties\":{\"foo\":\"bar\"}}"));
    }

}
