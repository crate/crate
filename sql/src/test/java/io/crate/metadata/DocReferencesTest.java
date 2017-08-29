/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata;

import io.crate.types.DataTypes;
import org.junit.Test;

import static org.junit.Assert.*;

public class DocReferencesTest {

    private static final TableIdent tableIdent = new TableIdent(Schemas.DOC_SCHEMA_NAME, "users");

    private static Reference stringRef(String path) {
        ColumnIdent columnIdent = ColumnIdent.fromPath(path);
        return new Reference(new ReferenceIdent(tableIdent, columnIdent), RowGranularity.DOC, DataTypes.STRING);
    }

    @Test
    public void testConvertDocReference() throws Exception {
        // users._doc['name'] -> users.name
        Reference reference = stringRef("_doc.name");
        Reference newRef = (Reference) DocReferences.inverseSourceLookup(reference);
        assertEquals(stringRef("name").ident(), newRef.ident());

        // users._doc -> users._doc
        reference = stringRef("_doc");
        newRef = (Reference) DocReferences.inverseSourceLookup(reference);
        assertEquals(stringRef("_doc").ident(), newRef.ident());
    }

    @Test
    public void testDontConvertOtherReferences() throws Exception {
        Reference reference = stringRef("_raw");
        Reference newRef = (Reference) DocReferences.inverseSourceLookup(reference);
        assertEquals(reference.ident(), newRef.ident());

        reference = stringRef("_id");
        newRef = (Reference) DocReferences.inverseSourceLookup(reference);
        assertEquals(reference.ident(), newRef.ident());

        reference = stringRef("address.zip_code");
        newRef = (Reference) DocReferences.inverseSourceLookup(reference);
        assertEquals(reference.ident(), newRef.ident());
    }
}
