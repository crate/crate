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

package io.crate.operation.rule.ingest;

import com.google.common.collect.Maps;
import io.crate.metadata.rule.ingest.IngestRule;
import io.crate.metadata.rule.ingest.IngestRulesMetaData;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.junit.Test;

public class TransportCreateDropTransferIngestRuleActionTest extends CrateUnitTest {

    @Test
    public void testCreateIngestRuleCreatesNewMetadataInstance() {
        MetaData.Builder mdBuilder = MetaData.builder();
        IngestRulesMetaData inputMetaData = new IngestRulesMetaData(Maps.newHashMap());
        mdBuilder.putCustom(IngestRulesMetaData.TYPE, inputMetaData);
        CreateIngestRuleRequest createRuleRequest = new CreateIngestRuleRequest("source",
            new IngestRule("ruleName", "table", "condition"));

        TransportCreateIngestRuleAction.createIngestRule(mdBuilder, createRuleRequest);

        IngestRulesMetaData updatedMetaData = (IngestRulesMetaData) mdBuilder.getCustom(IngestRulesMetaData.TYPE);
        assertNotSame(inputMetaData, updatedMetaData);
    }

    @Test
    public void testDropIngestRuleCreatesNewMetadataInstance() {
        MetaData.Builder mdBuilder = MetaData.builder();
        IngestRulesMetaData inputMetaData = new IngestRulesMetaData(Maps.newHashMap());
        inputMetaData.createIngestRule("someSource", new IngestRule("ruleName", "table", "condition"));
        mdBuilder.putCustom(IngestRulesMetaData.TYPE, inputMetaData);
        DropIngestRuleRequest dropIngestRuleRequest = new DropIngestRuleRequest("ruleName", false);

        TransportDropIngestRuleAction.dropIngestRule(mdBuilder, dropIngestRuleRequest);

        IngestRulesMetaData updatedMetaData = (IngestRulesMetaData) mdBuilder.getCustom(IngestRulesMetaData.TYPE);
        assertNotSame(inputMetaData, updatedMetaData);
    }

    @Test
    public void testDropIngestRulesForTableCreatesNewMetadataInstance() {
        MetaData.Builder mdBuilder = MetaData.builder();
        IngestRulesMetaData inputMetaData = new IngestRulesMetaData(Maps.newHashMap());
        inputMetaData.createIngestRule("someSource", new IngestRule("ruleName", "table", "condition"));
        mdBuilder.putCustom(IngestRulesMetaData.TYPE, inputMetaData);
        DropIngestRulesForTableRequest dropIngestRulesForTableRequest = new DropIngestRulesForTableRequest("doc.table");

        TransportDropIngestRulesForTableAction.dropIngestRulesForTable(mdBuilder, dropIngestRulesForTableRequest);

        IngestRulesMetaData updatedMetaData = (IngestRulesMetaData) mdBuilder.getCustom(IngestRulesMetaData.TYPE);
        assertNotSame(inputMetaData, updatedMetaData);
    }

    @Test
    public void testTransferIngestRuleCreatesNewMetadataInstance() {
        MetaData.Builder mdBuilder = MetaData.builder();
        IngestRulesMetaData inputMetaData = new IngestRulesMetaData(Maps.newHashMap());
        inputMetaData.createIngestRule("someSource", new IngestRule("ruleName", "table", "condition"));
        mdBuilder.putCustom(IngestRulesMetaData.TYPE, inputMetaData);
        TransferIngestRulesRequest transferIngestRulesRequest = new TransferIngestRulesRequest("table", "table2");

        TransportTransferIngestRulesAction.transferIngestRules(mdBuilder, transferIngestRulesRequest);

        IngestRulesMetaData updatedMetaData = (IngestRulesMetaData) mdBuilder.getCustom(IngestRulesMetaData.TYPE);
        assertNotSame(inputMetaData, updatedMetaData);
    }
}
