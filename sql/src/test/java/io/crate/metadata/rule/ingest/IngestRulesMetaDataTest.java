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
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.nullValue;

public class IngestRulesMetaDataTest extends CrateUnitTest {

    @Test
    public void testStreaming() throws IOException {
        Map<String, Set<IngestRule>> sourceRules = new HashMap<>();
        Set<IngestRule> rules = new HashSet<>();
        rules.add(new IngestRule("mqtt", "mqtt_raw", "topic like v4/%"));
        sourceRules.put("theMqttRule", rules);
        IngestRulesMetaData inputMetaData = new IngestRulesMetaData(sourceRules);

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

        Map<String, Set<IngestRule>> sourceRules = new HashMap<>();
        Set<IngestRule> rules = new HashSet<>();
        rules.add(new IngestRule("processV4TopicRule", "mqtt_raw", "topic like v4/%"));
        sourceRules.put("mqtt", rules);
        IngestRulesMetaData inputMetaData = new IngestRulesMetaData(sourceRules);

        inputMetaData.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(xContentRegistry(), builder.bytes());
        parser.nextToken(); // start object
        IngestRulesMetaData readMetaData = IngestRulesMetaData.fromXContent(parser);
        assertEquals(inputMetaData, readMetaData);

        // a metadata custom must consume the surrounded END_OBJECT token, no token must be left
        assertThat(parser.nextToken(), nullValue());
    }
}
