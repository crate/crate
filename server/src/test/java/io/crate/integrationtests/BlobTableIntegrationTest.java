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

package io.crate.integrationtests;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.testing.Asserts;
import io.crate.testing.TestingHelpers;

@IntegTestCase.ClusterScope(minNumDataNodes = 2)
public class BlobTableIntegrationTest extends SQLHttpIntegrationTest {

    @Test
    public void testJoinOnBlobTables() throws Exception {
        execute("create blob table b1 with (number_of_replicas = 0)");
        execute("create blob table b2 with (number_of_replicas = 0)");
        ensureYellow();

        blobUpload(new String[]{"bar", "foo", "baz"}, "b1", "b2");
        refresh();

        execute("select b1.digest from blob.b1 join blob.b2 on b1.digest = b2.digest");
        assertThat(TestingHelpers.printedTable(response.rows()),
            allOf(
                containsString("0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33\n"),
                containsString("62cdb7020ff920e5aa642c3d4066950dd1f01f4d\n"),
                containsString("bbe960a25ea311d21d40669e93df2003ba9b90a2\n")));
    }

    @Test
    public void testSortOnBlobTableColumn() throws Exception {
        execute("create blob table b1 with (number_of_replicas = 0)");
        ensureYellow();

        blobUpload(new String[]{"bar", "foo", "baz"}, "b1");
        refresh();

        execute("select b1.digest from blob.b1 order by b1.digest desc");
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("bbe960a25ea311d21d40669e93df2003ba9b90a2\n" +
               "62cdb7020ff920e5aa642c3d4066950dd1f01f4d\n" +
               "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33\n");
    }

    @Test
    public void testJoinAndSortOnBlobTables() throws Exception {
        execute("create blob table b1 with (number_of_replicas = 0)");
        execute("create blob table b2 with (number_of_replicas = 0)");
        ensureYellow();

        blobUpload(new String[]{"bar", "foo", "baz"}, "b1", "b2");
        refresh();

        execute("select b1.digest from blob.b1 join blob.b2 on b1.digest = b2.digest order by b1.digest");
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33\n" +
               "62cdb7020ff920e5aa642c3d4066950dd1f01f4d\n" +
               "bbe960a25ea311d21d40669e93df2003ba9b90a2\n");
    }

    @Test
    public void testJoinOnBlobAndDocTables() throws Exception {
        execute("create blob table b1 with (number_of_replicas = 0)");
        execute("create table files (digest string, i integer)");
        ensureYellow();

        blobUpload(new String[]{"bar", "foo", "baz"}, "b1");
        execute("insert into files (i, digest) values (?, ?)", new Object[][]{
            new Object[]{1, blobDigest("baz")},
            new Object[]{2, blobDigest("bar")},
            new Object[]{2, blobDigest("foo")}
        });
        refresh();

        execute("select b1.digest from blob.b1 " +
                "join files on b1.digest = files.digest " +
                "where files.i = 2 " +
                "order by files.digest");
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33\n" +
               "62cdb7020ff920e5aa642c3d4066950dd1f01f4d\n");
    }

    @Test
    public void testWhereOnBlobTable() throws Exception {
        execute("create blob table b1 with (number_of_replicas = 0)");
        ensureYellow();

        blobUpload(new String[]{"bar", "foo", "baz"}, "b1");
        refresh();

        execute("select digest from blob.b1 where digest = '62cdb7020ff920e5aa642c3d4066950dd1f01f4d'");
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("62cdb7020ff920e5aa642c3d4066950dd1f01f4d\n");
    }

    /**
     * https://github.com/crate/crate/issues/12534
     */
    @Test
    public void test_drop_tables_which_is_read_only() throws Exception {
        execute("create blob table b1 with (number_of_replicas = 0)");

        execute("alter blob table b1 set (\"blocks.read_only_allow_delete\"=true)");

        Asserts.assertSQLError(() -> execute("drop blob table b1"))
                .hasPGError(INTERNAL_ERROR)
                .hasHTTPError(INTERNAL_SERVER_ERROR, 5000)
                .hasMessageContaining("blocked by: ");

        execute("alter blob table b1 set (\"blocks.read_only_allow_delete\"=false)");
        execute("drop blob table b1");
        assertThat(response.rowCount()).isEqualTo(1L);
    }

    private void blobUpload(String[] contents, String... tables) throws Exception {
        for (String content : contents) {
            for (String table : tables) {
                upload(table, content);
            }
        }
    }
}
