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

package io.crate;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.Tuple;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class PartitionNameTest {

    @Test
    public void testSingleColumn() throws Exception {
        PartitionName partitionName = new PartitionName("test", ImmutableList.of(new BytesRef("1")));

        assertTrue(partitionName.isValid());
        assertThat(partitionName.values().size(), is(1));
        assertEquals(ImmutableList.of(new BytesRef("1")), partitionName.values());

        PartitionName partitionName1 = PartitionName.fromString(partitionName.stringValue(), "test");
        assertEquals(partitionName.values(), partitionName1.values());
    }

    @Test
    public void testWithoutValue() throws Exception {
        PartitionName partitionName = new PartitionName("test", ImmutableList.<BytesRef>of());
        assertFalse(partitionName.isValid());
    }

    @Test
    public void testMultipleColumns() throws Exception {
        PartitionName partitionName = new PartitionName("test", ImmutableList.of(new BytesRef("1"), new BytesRef("foo")));

        assertTrue(partitionName.isValid());
        assertThat(partitionName.values().size(), is(2));
        assertEquals(ImmutableList.of(new BytesRef("1"), new BytesRef("foo")), partitionName.values());

        PartitionName partitionName1 = PartitionName.fromString(partitionName.stringValue(), "test");
        assertEquals(partitionName.values(), partitionName1.values());
    }

    @Test
    public void testNull() throws Exception {
        PartitionName partitionName = new PartitionName("test", new ArrayList<BytesRef>(){{add(null);}});

        assertTrue(partitionName.isValid());
        assertThat(partitionName.values().size(), is(1));
        assertEquals(null, partitionName.values().get(0));

        PartitionName partitionName1 = PartitionName.fromString(partitionName.stringValue(), "test");
        assertEquals(partitionName.values(), partitionName1.values());
    }

    @Test
    public void testEmptyStringValue() throws Exception {
        PartitionName partitionName = new PartitionName("test", ImmutableList.of(new BytesRef("")));

        assertTrue(partitionName.isValid());
        assertThat(partitionName.values().size(), is(1));
        assertEquals(ImmutableList.of(new BytesRef("")), partitionName.values());

        PartitionName partitionName1 = PartitionName.fromString(partitionName.stringValue(), "test");
        assertEquals(partitionName.values(), partitionName1.values());
    }

    @Test
    public void testPartitionNameNotFromTable() throws Exception {
        String partitionName = Constants.PARTITIONED_TABLE_PREFIX + ".test1._1";
        assertFalse(PartitionName.tableName(partitionName).equals("test"));
    }

    @Test (expected = IllegalArgumentException.class)
    public void testInvalidValueString() throws Exception {
        String partitionName = Constants.PARTITIONED_TABLE_PREFIX + ".test.1";
        PartitionName.fromString(partitionName, "test");
    }

    @Test
    public void testIsPartition() throws Exception {
        assertFalse(
                PartitionName.isPartition("test", "test")
        );
        assertTrue(PartitionName.isPartition(
                Constants.PARTITIONED_TABLE_PREFIX + ".test.", "test"
        ));
        assertFalse(
                PartitionName.isPartition(
                        Constants.PARTITIONED_TABLE_PREFIX + ".tast.djfhjhdgfjy",
                        "test"
                )
        );
        assertFalse(
                PartitionName.isPartition("partitioned.test.dshhjfgjsdh", "test")
        );
        assertFalse(
                PartitionName.isPartition(".test.dshhjfgjsdh", "test")
        );
    }

    @Test
    public void testSplit() throws Exception {
        Tuple<String, String> tableNameValues = PartitionName.split(
                new PartitionName("t", Arrays.asList(new BytesRef("a"), new BytesRef("b"))).stringValue());
        assertThat(tableNameValues.v1(), is("t"));
        assertThat(tableNameValues.v2(), is("081620j2"));

        tableNameValues = PartitionName.split(
                new PartitionName("t", Arrays.asList(null, new BytesRef("b"))).stringValue());
        assertThat(tableNameValues.v1(), is("t"));
        assertThat(tableNameValues.v2(), is("08004og"));

        tableNameValues = PartitionName.split(
                new PartitionName("t",  new ArrayList<BytesRef>() {{ add(null); }}).stringValue());
        assertThat(tableNameValues.v1(), is("t"));
        assertThat(tableNameValues.v2(), is("0400"));

        tableNameValues = PartitionName.split(
                new PartitionName("t", Arrays.asList(new BytesRef("hoschi"))).stringValue());
        assertThat(tableNameValues.v1(), is("t"));
        assertThat(tableNameValues.v2(), is("043mgrrjcdk6i"));

    }

    @Test
    public void splitTemplateName() throws Exception {
        assertThat(
                PartitionName.split(PartitionName.templateName("t")).v1(),
                is("t"));
        assertThat(
                PartitionName.split(PartitionName.templateName("t")).v2(),
                is(""));

    }

    @Test(expected = IllegalArgumentException.class)
    public void testSplitInvalid1() throws Exception {
        PartitionName.split(Constants.PARTITIONED_TABLE_PREFIX + "lalala.n");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSplitInvalid2() throws Exception {
        PartitionName.split(Constants.PARTITIONED_TABLE_PREFIX.substring(1) + ".lalala.n");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSplitInvalid3() throws Exception {
        PartitionName.split("lalala");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSplitInvalid4() throws Exception {
        PartitionName.split(Constants.PARTITIONED_TABLE_PREFIX + ".lalala");
    }
}
