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

package io.crate.expression.reference.file;

import io.crate.metadata.ColumnIdent;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;
import java.nio.charset.StandardCharsets;

public class LineContextTest extends ESTestCase {
    @Test
    public void testGet() {
        LineContext context = new LineContext();

        String source = "{\"name\": \"foo\", \"details\": {\"age\": 43}}";
        context.rawSource(source.getBytes(StandardCharsets.UTF_8));

        assertNull(context.get(new ColumnIdent("invalid", "column")));
        assertNull(context.get(new ColumnIdent("details", "invalid")));
        assertEquals(43, context.get(new ColumnIdent("details", "age")));
    }
}
