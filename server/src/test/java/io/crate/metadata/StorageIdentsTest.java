/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.LongFunction;

import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class StorageIdentsTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_oids_in_lucene_error_messages_are_replaced_with_column_names() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (network text, o object as (y text))");
        DocTableInfo table = e.resolveTableInfo("tbl");
        Reference network = table.getReference(ColumnIdent.of("network"));
        Reference oy = table.getReference(ColumnIdent.of("o", "y"));
        LongFunction<ColumnIdent> resolveOid = resolveOid(table);

        assertThat(StorageIdents.replaceOids(
            "DocValuesField \"" + network.storageIdent() + "\" is too large, must be <= 32766",
            resolveOid))
            .isEqualTo("DocValuesField \"network\" is too large, must be <= 32766");

        assertThat(StorageIdents.replaceOids(
            "Document contains at least one immense term in field=\"" + oy.storageIdent()
                + "\" (whose UTF8 encoding is longer than the max length 32766)",
            resolveOid))
            .isEqualTo("Document contains at least one immense term in field=\"o['y']\""
                + " (whose UTF8 encoding is longer than the max length 32766)");
    }

    @Test
    public void test_oids_in_lucene_query_description_are_replaced_with_column_names() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (start_time timestamp with time zone)");
        DocTableInfo table = e.resolveTableInfo("tbl");
        Reference startTime = table.getReference(ColumnIdent.of("start_time"));

        assertThat(StorageIdents.replaceOids(
            startTime.storageIdent() + ":[-9223372036854775808 TO 1741790715]",
            resolveOid(table)))
            .isEqualTo("start_time:[-9223372036854775808 TO 1741790715]");
    }

    @Test
    public void test_digits_which_are_no_storage_idents_are_not_replaced() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x text)");
        DocTableInfo table = e.resolveTableInfo("tbl");
        LongFunction<ColumnIdent> resolveOid = resolveOid(table);

        assertThat(StorageIdents.replaceOids("bytes can be at most 32766 in length; got 93747", resolveOid))
            .isEqualTo("bytes can be at most 32766 in length; got 93747");
        assertThat(StorageIdents.replaceOids("field=\"99999\"", resolveOid))
            .isEqualTo("field=\"99999\"");
        assertThat(StorageIdents.replaceOids((String) null, resolveOid)).isNull();
    }

    @Test
    public void test_replace_oids_of_exception_keeps_type_and_cause() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (network text)");
        DocTableInfo table = e.resolveTableInfo("tbl");
        Reference network = table.getReference(ColumnIdent.of("network"));
        IllegalArgumentException error = new IllegalArgumentException(
            "DocValuesField \"" + network.storageIdent() + "\" is too large, must be <= 32766");

        assertThat(StorageIdents.replaceOids(error, table))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("DocValuesField \"network\" is too large, must be <= 32766")
            .hasCause(error);
    }

    @Test
    public void test_exceptions_without_storage_idents_are_returned_as_is() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (network text)");
        DocTableInfo table = e.resolveTableInfo("tbl");
        Exception error = new IllegalArgumentException("no storage ident in here");

        assertThat(StorageIdents.replaceOids(error, table)).isSameAs(error);
    }

    private static LongFunction<ColumnIdent> resolveOid(DocTableInfo table) {
        return oid -> {
            Reference ref = table.getReference(oid);
            return ref == null ? null : ref.column();
        };
    }
}
