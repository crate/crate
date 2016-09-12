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

package io.crate.metadata.blob;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.TableIdent;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;

public class BlobTableInfoTest extends CrateUnitTest {

    private BlobTableInfo info = new BlobTableInfo(
            new TableIdent("blob", "dummy"),
            "dummy",
            null,
            5,
            new BytesRef("0"),
            ImmutableMap.<String, Object>of(),
            new BytesRef("/tmp/blobs_path"));

    @Test
    public void testGetColumnInfo() throws Exception {
        Reference foobar = info.getReference(new ColumnIdent("digest"));
        assertNotNull(foobar);
        assertEquals(DataTypes.STRING, foobar.valueType());
    }

    @Test
    public void testPrimaryKey() throws Exception {
        assertEquals(Arrays.asList(new ColumnIdent[]{ new ColumnIdent("digest") }), info.primaryKey());
    }

    @Test
    public void testClusteredBy() throws Exception {
        assertEquals(new ColumnIdent("digest"), info.clusteredBy());
    }
}
