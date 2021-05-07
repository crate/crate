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

package io.crate.execution.engine.export;

import com.fasterxml.jackson.core.JsonGenerator;
import io.crate.metadata.ColumnIdent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.Is.is;

public class FileWriterCountCollectorTest extends ESTestCase {

    @Test
    public void testToNestedStringObjectMap() {
        Map<ColumnIdent, Object> columnIdentMap = new HashMap<>();
        columnIdentMap.put(new ColumnIdent("some", Arrays.asList("nested", "column")), "foo");
        Map<String, Object> convertedMap = FileWriterCountCollector.toNestedStringObjectMap(columnIdentMap);

        Map someMap = (Map) convertedMap.get("some");
        Map nestedMap = (Map) someMap.get("nested");
        assertThat(nestedMap.get("column"), is("foo"));
    }

    @Test
    public void testJsonBuilderDoesNotPassFlushToStream() throws Exception {
        Path file = createTempFile("out", "json");
        try (OutputStream os = new FileOutputStream(file.toFile())) {
            XContentBuilder xContentBuilder = FileWriterCountCollector.createJsonBuilder(os);
            assertThat(xContentBuilder.generator().isEnabled(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM),
                is(false));
        }
    }
}
