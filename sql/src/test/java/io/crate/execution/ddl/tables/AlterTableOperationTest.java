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

package io.crate.execution.ddl.tables;

import io.crate.Constants;
import io.crate.execution.ddl.tables.AlterTableOperation;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class AlterTableOperationTest extends CrateUnitTest {


    @Test
    public void testPrepareAlterTableMappingRequest() throws Exception {
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

        PutMappingRequest request = AlterTableOperation.preparePutMappingRequest(oldMapping, newMapping);

        assertThat(request.type(), is(Constants.DEFAULT_MAPPING_TYPE));
        assertThat(request.source(), is("{\"_meta\":{\"meta2\":\"v2\",\"meta1\":\"v1\"},\"properties\":{\"foo\":\"bar\"}}"));
    }

    @Test
    public void testOldSettingsAreArchivedOnPrepareIndexTemplateRequest() {
        IndexScopedSettings indexScopedSettings = new IndexScopedSettings(Settings.EMPTY, Collections.emptySet());

        String unsupportedSetting = "index.translog.disable_flush";
        Settings unsupportedSettings = Settings.builder()
            .put(unsupportedSetting, false)
            .build();
        IndexTemplateMetaData indexTemplateMetaData = IndexTemplateMetaData.builder("t1")
            .patterns(Collections.singletonList("*"))
            .settings(unsupportedSettings)
            .build();

        PutIndexTemplateRequest request = AlterTableOperation.preparePutIndexTemplateRequest(indexScopedSettings, indexTemplateMetaData,
            Collections.emptyMap(), Collections.emptyMap(), Settings.EMPTY, new RelationName(Schemas.DOC_SCHEMA_NAME, "t1"), "t1.*",
            logger);

        assertThat(request.settings().get(unsupportedSetting), nullValue());
        assertThat(request.settings().get(ARCHIVED_SETTINGS_PREFIX + unsupportedSetting), is("false"));
    }
}
