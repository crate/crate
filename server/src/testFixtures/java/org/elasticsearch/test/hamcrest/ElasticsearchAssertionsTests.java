/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.hamcrest;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

import java.io.IOException;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;

public class ElasticsearchAssertionsTests extends ESTestCase {

    public void testAssertXContentEquivalent() throws IOException {
        try (XContentBuilder original = JsonXContent.builder()) {
            original.startObject();
            for (Object value : RandomObjects.randomStoredFieldValues(random(), original.contentType()).v1()) {
                original.field(randomAlphaOfLength(10), value);
            }
            {
                original.startObject(randomAlphaOfLength(10));
                for (Object value : RandomObjects.randomStoredFieldValues(random(), original.contentType()).v1()) {
                    original.field(randomAlphaOfLength(10), value);
                }
                original.endObject();
            }
            {
                original.startArray(randomAlphaOfLength(10));
                for (Object value : RandomObjects.randomStoredFieldValues(random(), original.contentType()).v1()) {
                    original.value(value);
                }
                original.endArray();
            }
            original.endObject();

            try (XContentBuilder copy = JsonXContent.builder();
                    XContentParser parser = createParser(original.contentType().xContent(), BytesReference.bytes(original))) {
                parser.nextToken();
                copy.generator().copyCurrentStructure(parser);
                try (XContentBuilder copyShuffled = shuffleXContent(copy) ) {
                    assertToXContentEquivalent(BytesReference.bytes(original), BytesReference.bytes(copyShuffled), original.contentType());
                }
            }
        }
    }

    public void testAssertXContentEquivalentErrors() throws IOException {
        {
            XContentBuilder builder = JsonXContent.builder();
            builder.startObject();
            {
                builder.startObject("foo");
                {
                    builder.field("f1", "value1");
                    builder.field("f2", "value2");
                }
                builder.endObject();
            }
            builder.endObject();

            XContentBuilder otherBuilder = JsonXContent.builder();
            otherBuilder.startObject();
            {
                otherBuilder.startObject("foo");
                {
                    otherBuilder.field("f1", "value1");
                }
                otherBuilder.endObject();
            }
            otherBuilder.endObject();
            assertThatThrownBy(() -> assertToXContentEquivalent(
                BytesReference.bytes(builder),
                BytesReference.bytes(otherBuilder),
                builder.contentType())
            ).isExactlyInstanceOf(AssertionError.class)
                .hasMessageContaining("f2: expected [value2] but not found");
        }
        {
            XContentBuilder builder = JsonXContent.builder();
            builder.startObject();
            {
                builder.startObject("foo");
                {
                    builder.field("f1", "value1");
                    builder.field("f2", "value2");
                }
                builder.endObject();
            }
            builder.endObject();

            XContentBuilder otherBuilder = JsonXContent.builder();
            otherBuilder.startObject();
            {
                otherBuilder.startObject("foo");
                {
                    otherBuilder.field("f1", "value1");
                    otherBuilder.field("f2", "differentValue2");
                }
                otherBuilder.endObject();
            }
            otherBuilder.endObject();
            assertThatThrownBy(
                () -> assertToXContentEquivalent(BytesReference.bytes(builder), BytesReference.bytes(otherBuilder), builder.contentType()))
                .isExactlyInstanceOf(AssertionError.class)
                .hasMessageContaining("f2: expected [value2] but was [differentValue2]");
        }
        {
            XContentBuilder builder = JsonXContent.builder();
            builder.startObject();
            {
                builder.startArray("foo");
                {
                    builder.value("one");
                    builder.value("two");
                    builder.value("three");
                }
                builder.endArray();
            }
            builder.field("f1", "value");
            builder.endObject();

            XContentBuilder otherBuilder = JsonXContent.builder();
            otherBuilder.startObject();
            {
                otherBuilder.startArray("foo");
                {
                    otherBuilder.value("one");
                    otherBuilder.value("two");
                    otherBuilder.value("four");
                }
                otherBuilder.endArray();
            }
            otherBuilder.field("f1", "value");
            otherBuilder.endObject();
            assertThatThrownBy(() -> assertToXContentEquivalent(
                BytesReference.bytes(builder),
                BytesReference.bytes(otherBuilder),
                builder.contentType())
            ).isExactlyInstanceOf(AssertionError.class)
                .hasMessageContaining("2: expected [three] but was [four]");
        }
        {
            XContentBuilder builder = JsonXContent.builder();
            builder.startObject();
            {
                builder.startArray("foo");
                {
                    builder.value("one");
                    builder.value("two");
                    builder.value("three");
                }
                builder.endArray();
            }
            builder.endObject();

            XContentBuilder otherBuilder = JsonXContent.builder();
            otherBuilder.startObject();
            {
                otherBuilder.startArray("foo");
                {
                    otherBuilder.value("one");
                    otherBuilder.value("two");
                }
                otherBuilder.endArray();
            }
            otherBuilder.endObject();
            assertThatThrownBy(() -> assertToXContentEquivalent(
                BytesReference.bytes(builder),
                BytesReference.bytes(otherBuilder),
                builder.contentType())
            ).isExactlyInstanceOf(AssertionError.class)
                .hasMessageContaining("expected [1] more entries");
        }
    }
}
