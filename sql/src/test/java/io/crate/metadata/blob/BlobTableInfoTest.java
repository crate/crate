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

import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.symbol.DynamicReference;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class BlobTableInfoTest {

    static BlobTableInfo info = new BlobTableInfo(
            mock(BlobSchemaInfo.class),
            new TableIdent("blob", "dummy"),
            "dummy",
            null,
            5,
            new BytesRef("0"),
            new BytesRef("/tmp/blobs_path"));

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testGetColumnInfo() throws Exception {
        ReferenceInfo foobar = info.getReferenceInfo(new ColumnIdent("digest"));
        assertNotNull(foobar);
        assertEquals(DataTypes.STRING, foobar.type());
    }

    @Test
    public void testUnknownColumn() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        DynamicReference reference = info.dynamicReference(new ColumnIdent("foobar"));
        assertNull(reference);
    }

    @Test
    public void testPrimaryKey() throws Exception {
        assertEquals(Arrays.asList(new ColumnIdent[]{
                new ColumnIdent("digest")
        }), info.primaryKey());
    }

    @Test
    public void testClusteredBy() throws Exception {
        assertEquals(new ColumnIdent("digest"), info.clusteredBy());
    }

    @Test
    public void testAlias() throws Exception {
        assertFalse(info.isAlias());
    }
}
