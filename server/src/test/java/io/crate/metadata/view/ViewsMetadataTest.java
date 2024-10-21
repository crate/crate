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


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.RelationName;
import io.crate.metadata.SearchPath;

public class ViewsMetadataTest extends ESTestCase {

    public static ViewsMetadata createMetadata() {
        SearchPath searchPath1 = SearchPath.createSearchPathFrom("foo", "bar", "doc");
        Map<String, ViewMetadata> map = Map.of(
            "doc.my_view",
            new ViewMetadata("SELECT x, y FROM t1 WHERE z = 'a'", "user_a", searchPath1),
            "my_schema.other_view",
            new ViewMetadata("SELECT a, b FROM t2 WHERE c = 1", "user_b", searchPath1));
        return new ViewsMetadata(map);
    }

    @Test
    public void testViewsMetadataStreaming() throws IOException {
        ViewsMetadata views = createMetadata();
        BytesStreamOutput out = new BytesStreamOutput();
        views.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ViewsMetadata views2 = new ViewsMetadata(in);
        assertThat(views).isEqualTo(views2);
    }

    @Test
    public void test_raises_error_on_rename_if_source_is_missing() throws Exception {
        ViewsMetadata views = createMetadata();
        assertThatThrownBy(() -> views.rename(new RelationName("missing", "views"), new RelationName("doc", "v2")))
            .isExactlyInstanceOf(RelationUnknown.class);
    }

    @Test
    public void test_rename_creates_new_instance_with_old_view_removed_and_new_added() throws Exception {
        ViewsMetadata views = createMetadata();
        RelationName source = new RelationName("doc", "my_view");
        ViewMetadata sourceView = views.getView(source);
        RelationName target = new RelationName("doc", "v2");
        ViewsMetadata result = views.rename(source, target);
        assertThat(views).isNotSameAs(result);
        assertThat(result.contains(target)).isTrue();
        assertThat(result.contains(source)).isFalse();
        assertThat(result.getView(target)).isEqualTo(sourceView);
    }

}
