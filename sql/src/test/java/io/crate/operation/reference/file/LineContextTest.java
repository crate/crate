/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.reference.file;

import io.crate.metadata.ColumnIdent;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.ElasticsearchParseException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class LineContextTest extends CrateUnitTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testGet() throws Exception {
        LineContext context = new LineContext();

        String source = "{\"name\": \"foo\", \"details\": {\"age\": 43}}";
        context.rawSource(source.getBytes());

        assertNull(context.get(new ColumnIdent("invalid", "column")));
        assertNull(context.get(new ColumnIdent("details", "invalid")));
        assertEquals(43, context.get(new ColumnIdent("details", "age")));
    }

    @Test
    public void testInvalidJson() throws Exception {
        LineContext context = new LineContext();

        String source = "{|}";
        context.rawSource(source.getBytes());

        expectedException.expect(ElasticsearchParseException.class);
        expectedException.expectMessage("Failed to parse content to map");
        context.get(new ColumnIdent("invalid", "column"));
    }
}
