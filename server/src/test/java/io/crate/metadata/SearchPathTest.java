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

package io.crate.metadata;

import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

import static io.crate.metadata.SearchPath.pathWithPGCatalogAndDoc;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class SearchPathTest {

    @Test
    public void testEmptyConstructorSetsDefaultSchema() {
        Iterable<String> searchPath = pathWithPGCatalogAndDoc();
        Iterator<String> pathIterator = searchPath.iterator();
        pathIterator.next();
        String secondInPath = pathIterator.next();
        assertThat(secondInPath, is(Schemas.DOC_SCHEMA_NAME));
    }

    @Test
    public void testCurrentSchemaIsFirstSchemaInSearchPath() {
        SearchPath searchPath = SearchPath.createSearchPathFrom("firstSchema", "secondSchema");
        assertThat(searchPath.currentSchema(), is("firstSchema"));
    }

    @Test
    public void testPgCatalogIsFirstInTheSearchPathIfNotExplicitlySet() {
        SearchPath searchPath = SearchPath.createSearchPathFrom("firstSchema", "secondSchema");
        assertThat(searchPath.iterator().next(), is(PgCatalogSchemaInfo.NAME));
    }

    @Test
    public void testPgCatalogKeepsPositionInSearchPathWhenExplicitlySet() {
        SearchPath searchPath = SearchPath.createSearchPathFrom("firstSchema", "secondSchema", PgCatalogSchemaInfo.NAME);
        Iterator<String> pathIterator = searchPath.iterator();
        pathIterator.next();
        pathIterator.next();
        String thirdInPath = pathIterator.next();
        assertThat(thirdInPath, is(PgCatalogSchemaInfo.NAME));
    }

    @Test
    public void testPgCatalogISCurrentSchemaIfSetFirstInPath() {
        SearchPath searchPath = SearchPath.createSearchPathFrom(PgCatalogSchemaInfo.NAME, "secondSchema");
        assertThat(searchPath.currentSchema(), is(PgCatalogSchemaInfo.NAME));
    }

    @Test
    public void testSearchPathStreaming() throws IOException {
        SearchPath s1 = SearchPath.createSearchPathFrom(PgCatalogSchemaInfo.NAME, "secondSchema");
        BytesStreamOutput out = new BytesStreamOutput();
        s1.writeTo(out);

        SearchPath s2 = SearchPath.createSearchPathFrom(out.bytes().streamInput());
        assertEquals(s1, s2);
    }

    @Test
    public void testSearchPathPgCatalogNotExplicitlySetStreaming() throws IOException {
        SearchPath s1 = SearchPath.createSearchPathFrom("firstSchema");
        BytesStreamOutput out = new BytesStreamOutput();
        s1.writeTo(out);

        SearchPath s2 = SearchPath.createSearchPathFrom(out.bytes().streamInput());
        assertEquals(s1, s2);
    }
}
