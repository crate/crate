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

package io.crate.metadata.view;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.core.IsNull.nullValue;

public class ViewsMetadataTest extends ESTestCase {

    public static ViewsMetadata createMetadata() {
        Map<String, ViewMetadata> map = Map.of(
            "doc.my_view",
            new ViewMetadata("SELECT x, y FROM t1 WHERE z = 'a'", "user_a"),
            "my_schema.other_view",
            new ViewMetadata("SELECT a, b FROM t2 WHERE c = 1", "user_b"));
        return new ViewsMetadata(map);
    }

    @Test
    public void testViewsMetadataStreaming() throws IOException {
        ViewsMetadata views = createMetadata();
        BytesStreamOutput out = new BytesStreamOutput();
        views.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ViewsMetadata views2 = new ViewsMetadata(in);
        assertEquals(views, views2);

    }

    @Test
    public void testViewsMetadataToXContent() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        // reflects the logic used to process custom metadata in the cluster state
        builder.startObject();

        ViewsMetadata views = createMetadata();
        views.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
            xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            BytesReference.toBytes(BytesReference.bytes(builder)));
        parser.nextToken(); // start object
        ViewsMetadata views2 = ViewsMetadata.fromXContent(parser);
        assertEquals(views, views2);

        // a metadata custom must consume the surrounded END_OBJECT token, no token must be left
        assertThat(parser.nextToken(), nullValue());
    }

}
