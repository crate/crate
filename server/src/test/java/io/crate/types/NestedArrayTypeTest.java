/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.types;

import static io.crate.execution.dml.IndexerTest.getIndexer;
import static io.crate.testing.Asserts.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.junit.Test;

import io.crate.execution.dml.IndexItem;
import io.crate.execution.dml.Indexer;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.testing.DataTypeTesting;
import io.crate.testing.SQLExecutor;

public class NestedArrayTypeTest extends DataTypeTestCase<List<List<Object>>> {

    @Override
    @SuppressWarnings("unchecked")
    public DataType<List<List<Object>>> getType() {
        DataType<Object> randomType = (DataType<Object>) DataTypeTesting.randomTypeExcluding(
            Set.of(FloatVectorType.INSTANCE_ONE)
        );
        return new ArrayType<>(new ArrayType<>(randomType));
    }

    @Test
    public void test_index_structure() throws IOException {
        // create a table with a nested array
        var sqlExecutor = SQLExecutor.of(clusterService)
            .addTable("create table tbl (id int, x int[][])");
        DocTableInfo table = sqlExecutor.resolveTableInfo("tbl");

        // Parse a document using the table schema
        Indexer indexer = getIndexer(sqlExecutor, table.ident().name(), "x");
        Object[] insertValues = new Object[] { (List.of(List.of(1, 2), List.of(3, 4))) };
        ParsedDocument doc
            = indexer.index(new IndexItem.StaticItem("id", List.of(), insertValues, 0, 0));

        // Leaf values are stored as individual int points + docvalues
        Document expected = new Document();
        String resolvedField = table.getReference(ColumnIdent.fromPath("x")).storageIdent();
        expected.add(new IntField(resolvedField, 1, Field.Store.NO));
        expected.add(new IntField(resolvedField, 2, Field.Store.NO));
        expected.add(new IntField(resolvedField, 3, Field.Store.NO));
        expected.add(new IntField(resolvedField, 4, Field.Store.NO));
        assertThat(doc).hasSameResolvedFields(expected, resolvedField);

        // Source stores the original nested array structure
        assertThat(doc.source().utf8ToString()).isEqualTo("{\"" + resolvedField + "\":[[1,2],[3,4]]}");
    }
}
