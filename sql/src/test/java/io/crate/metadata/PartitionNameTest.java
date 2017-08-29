/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.ImmutableList;
import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

public class PartitionNameTest extends CrateUnitTest {

    @Test
    public void testSingleColumn() throws Exception {
        PartitionName partitionName = new PartitionName("test", ImmutableList.of(new BytesRef("1")));

        assertThat(partitionName.values().size(), is(1));
        assertEquals(ImmutableList.of(new BytesRef("1")), partitionName.values());

        PartitionName partitionName1 = PartitionName.fromIndexOrTemplate(partitionName.asIndexName());
        assertEquals(partitionName.values(), partitionName1.values());
    }

    @Test
    public void testSingleColumnSchema() throws Exception {
        PartitionName partitionName = new PartitionName("schema", "test", ImmutableList.of(new BytesRef("1")));

        assertThat(partitionName.values().size(), is(1));
        assertEquals(ImmutableList.of(new BytesRef("1")), partitionName.values());

        PartitionName partitionName1 = PartitionName.fromIndexOrTemplate(partitionName.asIndexName());
        assertEquals(partitionName.values(), partitionName1.values());
    }

    @Test
    public void testMultipleColumns() throws Exception {
        PartitionName partitionName = new PartitionName("test", ImmutableList.of(new BytesRef("1"), new BytesRef("foo")));

        assertThat(partitionName.values().size(), is(2));
        assertEquals(ImmutableList.of(new BytesRef("1"), new BytesRef("foo")), partitionName.values());

        PartitionName partitionName1 = PartitionName.fromIndexOrTemplate(partitionName.asIndexName());
        assertEquals(partitionName.values(), partitionName1.values());
    }

    @Test
    public void testMultipleColumnsSchema() throws Exception {
        PartitionName partitionName = new PartitionName("schema", "test", ImmutableList.of(new BytesRef("1"), new BytesRef("foo")));

        assertThat(partitionName.values().size(), is(2));
        assertEquals(ImmutableList.of(new BytesRef("1"), new BytesRef("foo")), partitionName.values());

        PartitionName partitionName1 = PartitionName.fromIndexOrTemplate(partitionName.asIndexName());
        assertEquals(partitionName.values(), partitionName1.values());
    }

    @Test
    public void testNull() throws Exception {
        PartitionName partitionName = new PartitionName("test", new ArrayList<BytesRef>() {{
            add(null);
        }});

        assertThat(partitionName.values().size(), is(1));
        assertEquals(null, partitionName.values().get(0));

        PartitionName partitionName1 = PartitionName.fromIndexOrTemplate(partitionName.asIndexName());
        assertEquals(partitionName.values(), partitionName1.values());
    }

    @Test
    public void testNullSchema() throws Exception {
        PartitionName partitionName = new PartitionName("schema", "test", new ArrayList<BytesRef>() {{
            add(null);
        }});

        assertThat(partitionName.values().size(), is(1));
        assertEquals(null, partitionName.values().get(0));

        PartitionName partitionName1 = PartitionName.fromIndexOrTemplate(partitionName.asIndexName());
        assertEquals(partitionName.values(), partitionName1.values());
    }

    @Test
    public void testEmptyStringValue() throws Exception {
        PartitionName partitionName = new PartitionName("test", ImmutableList.of(new BytesRef("")));

        assertThat(partitionName.values().size(), is(1));
        assertEquals(ImmutableList.of(new BytesRef("")), partitionName.values());

        PartitionName partitionName1 = PartitionName.fromIndexOrTemplate(partitionName.asIndexName());
        assertEquals(partitionName.values(), partitionName1.values());
    }

    @Test
    public void testPartitionNameNotFromTable() throws Exception {
        String partitionName = PartitionName.PARTITIONED_TABLE_PREFIX + ".test1._1";
        assertFalse(PartitionName.fromIndexOrTemplate(partitionName).tableIdent().name().equals("test"));
    }

    @Test
    public void testPartitionNameNotFromSchema() throws Exception {
        String partitionName = "schema1." + PartitionName.PARTITIONED_TABLE_PREFIX + ".test1._1";
        assertFalse(PartitionName.fromIndexOrTemplate(partitionName).tableIdent().schema().equals("schema"));
    }

    @Test
    public void testInvalidValueString() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid partition ident: 1");

        String partitionName = PartitionName.PARTITIONED_TABLE_PREFIX + ".test.1";
        PartitionName.fromIndexOrTemplate(partitionName).values();
    }

    @Test
    public void testIsPartition() throws Exception {
        assertFalse(PartitionName.isPartition("test"));

        assertTrue(PartitionName.isPartition(PartitionName.PARTITIONED_TABLE_PREFIX + ".test."));
        assertTrue(PartitionName.isPartition("schema." + PartitionName.PARTITIONED_TABLE_PREFIX + ".test."));

        assertFalse(PartitionName.isPartition("partitioned.test.dshhjfgjsdh"));
        assertFalse(PartitionName.isPartition("schema.partitioned.test.dshhjfgjsdh"));
        assertFalse(PartitionName.isPartition(".test.dshhjfgjsdh"));
        assertFalse(PartitionName.isPartition("schema.test.dshhjfgjsdh"));
        assertTrue(PartitionName.isPartition(".partitioned.test.dshhjfgjsdh"));
        assertTrue(PartitionName.isPartition("schema..partitioned.test.dshhjfgjsdh"));
    }

    @Test
    public void testFromIndexOrTemplate() throws Exception {
        PartitionName partitionName = new PartitionName("t", Arrays.asList(new BytesRef("a"), new BytesRef("b")));
        assertThat(partitionName, equalTo(PartitionName.fromIndexOrTemplate(partitionName.asIndexName())));

        partitionName = new PartitionName("t", Arrays.asList(new BytesRef("a"), new BytesRef("b")));
        assertThat(partitionName, equalTo(PartitionName.fromIndexOrTemplate(partitionName.asIndexName())));
        assertThat(partitionName.ident(), is("081620j2"));

        partitionName = new PartitionName("schema", "t", Arrays.asList(new BytesRef("a"), new BytesRef("b")));
        assertThat(partitionName, equalTo(PartitionName.fromIndexOrTemplate(partitionName.asIndexName())));
        assertThat(partitionName.ident(), is("081620j2"));

        partitionName = new PartitionName( "t", Collections.singletonList(new BytesRef("hoschi")));
        assertThat(partitionName, equalTo(PartitionName.fromIndexOrTemplate(partitionName.asIndexName())));
        assertThat(partitionName.ident(), is("043mgrrjcdk6i"));

        partitionName = new PartitionName("t", Collections.singletonList(null));
        assertThat(partitionName, equalTo(PartitionName.fromIndexOrTemplate(partitionName.asIndexName())));
        assertThat(partitionName.ident(), is("0400"));
    }

    @Test
    public void splitTemplateName() throws Exception {
        PartitionName partitionName = PartitionName.fromIndexOrTemplate(PartitionName.templateName("schema", "t"));
        assertThat(partitionName.tableIdent(), is(new TableIdent("schema", "t")));
        assertThat(partitionName.ident(), is(""));
    }

    @Test
    public void testSplitInvalid1() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid partition name");
        PartitionName.fromIndexOrTemplate(PartitionName.PARTITIONED_TABLE_PREFIX + "lalala.n");
    }

    @Test
    public void testSplitInvalid2() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid partition name");
        PartitionName.fromIndexOrTemplate(PartitionName.PARTITIONED_TABLE_PREFIX.substring(1) + ".lalala.n");
    }

    @Test
    public void testSplitInvalid3() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid partition name");
        PartitionName.fromIndexOrTemplate("lalala");
    }

    @Test
    public void testSplitInvalid4() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid partition name");
        PartitionName.fromIndexOrTemplate(PartitionName.PARTITIONED_TABLE_PREFIX + ".lalala");
    }

    @Test
    public void testSplitInvalidWithSchema1() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid partition name");
        PartitionName.fromIndexOrTemplate("schema" + PartitionName.PARTITIONED_TABLE_PREFIX + ".lalala");
    }

    @Test
    public void testSplitInvalidWithSchema2() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid partition name");
        PartitionName.fromIndexOrTemplate("schema." + PartitionName.PARTITIONED_TABLE_PREFIX + ".lalala");
    }

    @Test
    public void testEquals() throws Exception {
        assertTrue(
            new PartitionName("table", Arrays.asList(new BytesRef("xxx"))).equals(
                new PartitionName("table", Arrays.asList(new BytesRef("xxx")))));
        assertTrue(
            new PartitionName("table", Arrays.asList(new BytesRef("xxx"))).equals(
                new PartitionName("table", Arrays.asList(new BytesRef("xxx")))));
        assertFalse(
            new PartitionName("table", Arrays.asList(new BytesRef("xxx"))).equals(
                new PartitionName("schema", "table", Arrays.asList(new BytesRef("xxx")))));
        PartitionName name = new PartitionName( "table", Arrays.asList(new BytesRef("xxx")));
        assertTrue(name.equals(PartitionName.fromIndexOrTemplate(name.asIndexName())));
    }
}
