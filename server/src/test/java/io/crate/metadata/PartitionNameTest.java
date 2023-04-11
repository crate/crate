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

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class PartitionNameTest extends ESTestCase {

    @Test
    public void testSingleColumn() throws Exception {
        PartitionName partitionName = new PartitionName(new RelationName("doc", "test"), List.of("1"));

        assertThat(partitionName.values()).hasSize(1);
        assertThat(partitionName.values()).isEqualTo(List.of("1"));

        PartitionName partitionName1 = PartitionName.fromIndexOrTemplate(partitionName.asIndexName());
        assertThat(partitionName1.values()).isEqualTo(partitionName.values());
    }

    @Test
    public void testSingleColumnSchema() throws Exception {
        PartitionName partitionName = new PartitionName(new RelationName("schema", "test"), List.of("1"));

        assertThat(partitionName.values()).hasSize(1);
        assertThat(partitionName.values()).isEqualTo(List.of("1"));

        PartitionName partitionName1 = PartitionName.fromIndexOrTemplate(partitionName.asIndexName());
        assertThat(partitionName1.values()).isEqualTo(partitionName.values());
    }

    @Test
    public void testMultipleColumns() throws Exception {
        PartitionName partitionName = new PartitionName(
            new RelationName("doc", "test"),
            List.of("1", "foo")
        );

        assertThat(partitionName.values()).hasSize(2);
        assertThat(partitionName.values()).isEqualTo(List.of("1", "foo"));

        PartitionName partitionName1 = PartitionName.fromIndexOrTemplate(partitionName.asIndexName());
        assertThat(partitionName1.values()).isEqualTo(partitionName.values());
    }

    @Test
    public void testMultipleColumnsSchema() throws Exception {
        PartitionName partitionName = new PartitionName(
            new RelationName("schema", "test"), List.of("1", "foo"));

        assertThat(partitionName.values()).hasSize(2);
        assertThat(partitionName.values()).isEqualTo(List.of("1", "foo"));

        PartitionName partitionName1 = PartitionName.fromIndexOrTemplate(partitionName.asIndexName());
        assertThat(partitionName1.values()).isEqualTo(partitionName.values());
    }

    @Test
    public void testNull() throws Exception {
        PartitionName partitionName = new PartitionName(new RelationName("doc", "test"), singletonList(null));

        assertThat(partitionName.values()).hasSize(1);
        assertThat(partitionName.values().get(0)).isEqualTo(null);

        PartitionName partitionName1 = PartitionName.fromIndexOrTemplate(partitionName.asIndexName());
        assertThat(partitionName1.values()).isEqualTo(partitionName.values());
    }

    @Test
    public void testNullSchema() throws Exception {
        PartitionName partitionName = new PartitionName(new RelationName("schema", "test"), singletonList(null));
        assertThat(partitionName.values()).hasSize(1);
        assertThat(partitionName.values().get(0)).isEqualTo(null);

        PartitionName partitionName1 = PartitionName.fromIndexOrTemplate(partitionName.asIndexName());
        assertThat(partitionName1.values()).isEqualTo(partitionName.values());
    }

    @Test
    public void testEmptyStringValue() throws Exception {
        PartitionName partitionName = new PartitionName(new RelationName("doc", "test"), List.of(""));

        assertThat(partitionName.values()).hasSize(1);
        assertThat(partitionName.values()).isEqualTo(List.of(""));

        PartitionName partitionName1 = PartitionName.fromIndexOrTemplate(partitionName.asIndexName());
        assertThat(partitionName1.values()).isEqualTo(partitionName.values());
    }

    @Test
    public void testPartitionNameNotFromTable() throws Exception {
        String partitionName = IndexParts.PARTITIONED_TABLE_PART + "test1._1";
        assertThat(PartitionName.fromIndexOrTemplate(partitionName).relationName().name().equals("test")).isFalse();
    }

    @Test
    public void testPartitionNameNotFromSchema() throws Exception {
        String partitionName = "schema1." + IndexParts.PARTITIONED_TABLE_PART + "test1._1";
        assertThat(PartitionName.fromIndexOrTemplate(partitionName).relationName().schema().equals("schema")).isFalse();
    }

    @Test
    public void testInvalidValueString() throws Exception {
        String partitionName = IndexParts.PARTITIONED_TABLE_PART + "test.🐩";
        assertThatThrownBy(() -> PartitionName.fromIndexOrTemplate(partitionName).values())
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid partition ident: 🐩");
    }

    @Test
    public void testIsPartition() throws Exception {
        assertThat(IndexParts.isPartitioned("test")).isFalse();

        assertThat(IndexParts.isPartitioned(IndexParts.PARTITIONED_TABLE_PART + "test.")).isTrue();
        assertThat(IndexParts.isPartitioned("schema." + IndexParts.PARTITIONED_TABLE_PART + "test.")).isTrue();

        assertThat(IndexParts.isPartitioned("partitioned.test.dshhjfgjsdh")).isFalse();
        assertThat(IndexParts.isPartitioned("schema.partitioned.test.dshhjfgjsdh")).isFalse();
        assertThat(IndexParts.isPartitioned(".test.dshhjfgjsdh")).isFalse();
        assertThat(IndexParts.isPartitioned("schema.test.dshhjfgjsdh")).isFalse();
        assertThat(IndexParts.isPartitioned(".partitioned.test.dshhjfgjsdh")).isTrue();
        assertThat(IndexParts.isPartitioned("schema..partitioned.test.dshhjfgjsdh")).isTrue();
    }

    @Test
    public void testFromIndexOrTemplate() throws Exception {
        PartitionName partitionName = new PartitionName(
            new RelationName("doc", "t"), Arrays.asList("a", "b"));
        assertThat(partitionName).isEqualTo(PartitionName.fromIndexOrTemplate(partitionName.asIndexName()));

        partitionName = new PartitionName(
            new RelationName("doc", "t"), Arrays.asList("a", "b"));
        assertThat(partitionName).isEqualTo(PartitionName.fromIndexOrTemplate(partitionName.asIndexName()));
        assertThat(partitionName.ident()).isEqualTo("081620j2");

        partitionName = new PartitionName(
            new RelationName("schema", "t"), Arrays.asList("a", "b"));
        assertThat(partitionName).isEqualTo(PartitionName.fromIndexOrTemplate(partitionName.asIndexName()));
        assertThat(partitionName.ident()).isEqualTo("081620j2");

        partitionName = new PartitionName(
            new RelationName("doc", "t"), singletonList("hoschi"));
        assertThat(partitionName).isEqualTo(PartitionName.fromIndexOrTemplate(partitionName.asIndexName()));
        assertThat(partitionName.ident()).isEqualTo("043mgrrjcdk6i");

        partitionName = new PartitionName(
            new RelationName("doc", "t"), singletonList(null));
        assertThat(partitionName).isEqualTo(PartitionName.fromIndexOrTemplate(partitionName.asIndexName()));
        assertThat(partitionName.ident()).isEqualTo("0400");
    }

    @Test
    public void splitTemplateName() throws Exception {
        PartitionName partitionName = PartitionName.fromIndexOrTemplate(PartitionName.templateName("schema", "t"));
        assertThat(partitionName.relationName()).isEqualTo(new RelationName("schema", "t"));
        assertThat(partitionName.ident()).isEqualTo("");
    }

    @Test
    public void testSplitInvalid1() throws Exception {
        String part = IndexParts.PARTITIONED_TABLE_PART.substring(0, IndexParts.PARTITIONED_TABLE_PART.length() - 1);
        assertThatThrownBy(() -> PartitionName.fromIndexOrTemplate(part + "lalala.n"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageStartingWith("Invalid index name");
    }

    @Test
    public void testSplitInvalid2() throws Exception {
        String indexOrTemplate = IndexParts.PARTITIONED_TABLE_PART.substring(1) + "lalala.n";
        assertThatThrownBy(() -> PartitionName.fromIndexOrTemplate(indexOrTemplate))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageStartingWith("Invalid index name");
    }

    @Test
    public void testSplitInvalid3() throws Exception {
        assertThatThrownBy(() -> PartitionName.fromIndexOrTemplate("lalala"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Trying to create partition name from the name of a non-partitioned table lalala");
    }

    @Test
    public void testSplitInvalid4() throws Exception {
        String indexOrTemplate = IndexParts.PARTITIONED_TABLE_PART + "lalala";
        assertThatThrownBy(() -> PartitionName.fromIndexOrTemplate(indexOrTemplate))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageStartingWith("Invalid index name");
    }

    @Test
    public void testSplitInvalidWithSchema1() throws Exception {
        String indexOrTemplate = "schema" + IndexParts.PARTITIONED_TABLE_PART + "lalala";
        assertThatThrownBy(() -> PartitionName.fromIndexOrTemplate(indexOrTemplate))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageStartingWith("Invalid index name");
    }

    @Test
    public void testSplitInvalidWithSchema2() throws Exception {
        String indexOrTemplate = "schema." + IndexParts.PARTITIONED_TABLE_PART + "lalala";
        assertThatThrownBy(() -> PartitionName.fromIndexOrTemplate(indexOrTemplate))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid index name: schema");
    }

    @Test
    public void testEquals() throws Exception {
        assertThat(new PartitionName(new RelationName("doc", "table"), Arrays.asList("xxx")))
            .isEqualTo(new PartitionName(new RelationName("doc", "table"), Arrays.asList("xxx")));
        assertThat(new PartitionName(new RelationName("doc", "table"), Arrays.asList("xxx")))
            .isEqualTo(new PartitionName(new RelationName("doc", "table"), Arrays.asList("xxx")));
        assertThat(new PartitionName(new RelationName("doc", "table"), Arrays.asList("xxx")))
            .isNotEqualTo(new PartitionName(new RelationName("schema", "table"), Arrays.asList("xxx")));
        PartitionName name = new PartitionName(new RelationName("doc", "table"), Arrays.asList("xxx"));
        assertThat(name.equals(PartitionName.fromIndexOrTemplate(name.asIndexName()))).isTrue();
    }
}
