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

package io.crate.metadata.rule.ingest;

import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class IngestRulesMetaDataTest extends CrateUnitTest {

    private static final String SOURCE_NAME = "mqtt";
    private static final String RULE_NAME = "processV4TopicRule";
    private IngestRulesMetaData inputMetaData;

    @Before
    public void setupIngestRulesMetaData() {
        Map<String, Set<IngestRule>> sourceRules = new HashMap<>();
        Set<IngestRule> rules = new HashSet<>();
        rules.add(new IngestRule(RULE_NAME, "mqtt_raw", "topic like v4/%"));
        sourceRules.put(SOURCE_NAME, rules);
        inputMetaData = new IngestRulesMetaData(sourceRules);
    }

    @Test
    public void testStreaming() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        inputMetaData.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        IngestRulesMetaData readMetaData = new IngestRulesMetaData(in);
        assertEquals(inputMetaData, readMetaData);
    }

    @Test
    public void testXContentSerialization() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        inputMetaData.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(xContentRegistry(), builder.bytes());
        parser.nextToken(); // start object
        IngestRulesMetaData readMetaData = IngestRulesMetaData.fromXContent(parser);
        assertEquals(inputMetaData, readMetaData);

        // a metadata custom must consume the surrounded END_OBJECT token, no token must be left
        assertThat(parser.nextToken(), nullValue());
    }

    @Test
    public void testGetIngestRulesForValidSourceReturnsIngestRules() {
        Set<IngestRule> ingestRules = inputMetaData.getIngestRules(SOURCE_NAME);
        assertThat(ingestRules.size(), is(1));
        assertThat(ingestRules.iterator().next().getName(), is(RULE_NAME));
    }

    @Test
    public void testGetIngestRulesForMissingSourceReturnsNull() {
        assertThat(inputMetaData.getIngestRules("some_missing_source"), nullValue());
    }

    @Test
    public void testCreateIngestRule() {
        inputMetaData.createIngestRule("new_source", new IngestRule("new_rule", "log", "topic like log%"));

        Set<IngestRule> newSourceRules = inputMetaData.getIngestRules("new_source");
        assertThat(newSourceRules, notNullValue());
        assertThat(newSourceRules.size(), is(1));
        assertThat(newSourceRules.iterator().next().getName(), is("new_rule"));
    }

    @Test
    public void testCreateExistingRuleThrowsIllegalArgumentException() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Ingest rule with name " + RULE_NAME + " already exists");
        inputMetaData.createIngestRule("new_source", new IngestRule(RULE_NAME, "log", "topic like log%"));
    }

    @Test
    public void testDropTheOnlyExistingIngestRuleReturnsEmptyCollection() {
        inputMetaData.dropIngestRule(RULE_NAME, false);
        assertThat(inputMetaData.getIngestRules(SOURCE_NAME), empty());
    }

    @Test
    public void testDropMissingIngestRuleThrowsIllegalArgumentException() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Ingest rule missing_rule doesn't exist");
        inputMetaData.dropIngestRule("missing_rule", false);
    }

    @Test
    public void testDropMissingIngestRuleIfExistsDoesntThrowsException() {
        inputMetaData.dropIngestRule("missing_rule", true);
    }

    @Test
    public void testCopyOfNullReturnsNewInstance() {
        assertThat(IngestRulesMetaData.copyOf(null), notNullValue());
    }

    @Test
    public void testCopyOf() {
        IngestRulesMetaData copyOfMetaData = IngestRulesMetaData.copyOf(inputMetaData);
        assertNotSame(copyOfMetaData, inputMetaData);
    }

    @Test
    public void testGetAllRulesForTargetTable() {
        IngestRule newIngestRule = new IngestRule("newRule", "mqtt_table", "topic = 'test'");
        inputMetaData.createIngestRule("newSource", newIngestRule);

        Set<IngestRule> allRules = inputMetaData.getAllRulesForTargetTable("mqtt_table");
        assertThat(allRules.size(), is(1));
        assertThat(allRules.iterator().next(), is(newIngestRule));
    }

    @Test
    public void testDropIngestRulesForTable() {
        long affectedRows = inputMetaData.dropIngestRulesForTable("mqtt_raw");
        assertThat(affectedRows, is(1L));
    }
}
