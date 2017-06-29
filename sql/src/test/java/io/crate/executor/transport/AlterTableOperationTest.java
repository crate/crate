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

package io.crate.executor.transport;

import io.crate.Constants;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

public class AlterTableOperationTest extends CrateUnitTest {

    @Test
    public void testPrepareRenameTemplateRequests() throws Exception {
        TableIdent sourceIdent = new TableIdent(null, "users_old");
        TableIdent targetIdent = new TableIdent(null, "users");

        Settings settings = Settings.builder().put("some_setting", true).build();
        MetaData metaData = MetaData.builder()
            .put(IndexTemplateMetaData.builder(PartitionName.templateName(sourceIdent.schema(), sourceIdent.name()))
                .template(PartitionName.templatePrefix(sourceIdent.schema(), sourceIdent.name()))
                .putAlias(AliasMetaData.newAliasMetaDataBuilder(sourceIdent.name()))
                .putMapping(Constants.DEFAULT_MAPPING_TYPE, "{\"default\":{\"foo\":\"bar\"}}")
                .settings(settings)
            ).build();

        Tuple<PutIndexTemplateRequest, DeleteIndexTemplateRequest> requests =
            AlterTableOperation.prepareRenameTemplateRequests(metaData, sourceIdent, targetIdent);

        PutIndexTemplateRequest addRequest = requests.v1();
        assertThat(addRequest.create(), is(true));
        assertThat(addRequest.name(), is(PartitionName.templateName(targetIdent.schema(), targetIdent.name())));
        assertThat(addRequest.template(), is(PartitionName.templatePrefix(targetIdent.schema(), targetIdent.name())));
        assertThat(addRequest.settings(), is(settings));
        assertThat(addRequest.mappings().get(Constants.DEFAULT_MAPPING_TYPE), is("{\"default\":{\"foo\":\"bar\"}}"));
        List<String> aliases = addRequest.aliases().stream().map(Alias::name).collect(Collectors.toList());
        assertThat(aliases, hasItem(targetIdent.name()));

        assertThat(requests.v2().name(), is(PartitionName.templateName(sourceIdent.schema(), sourceIdent.name())));
    }
}
