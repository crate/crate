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

package io.crate.metadata.blob;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

import java.util.Collections;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.types.DataTypes;

public class BlobTableInfoTest extends ESTestCase {

    private BlobTableInfo info = new BlobTableInfo(
        new RelationName("blob", "dummy"),
        ".blob_dummy",
        5,
        "0",
        Settings.EMPTY,
        "/tmp/blobs_path",
        Version.CURRENT,
        null,
        false);

    @Test
    public void testGetColumnInfo() throws Exception {
        Reference foobar = info.getReference(ColumnIdent.of("digest"));
        assertNotNull(foobar);
        assertThat(foobar.valueType()).isEqualTo(DataTypes.STRING);
    }

    @Test
    public void testPrimaryKey() throws Exception {
        assertThat(info.primaryKey()).isEqualTo(Collections.singletonList(ColumnIdent.of("digest")));
    }

    @Test
    public void testClusteredBy() throws Exception {
        assertThat(info.clusteredBy()).isEqualTo(ColumnIdent.of("digest"));
    }
}
