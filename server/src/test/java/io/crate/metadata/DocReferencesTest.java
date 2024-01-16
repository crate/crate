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


import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import io.crate.common.collections.Lists;
import io.crate.types.DataTypes;

public class DocReferencesTest {

    private static final RelationName RELATION_ID = new RelationName(Schemas.DOC_SCHEMA_NAME, "users");

    private static Reference stringRef(String path) {
        ColumnIdent columnIdent = ColumnIdent.fromPath(path);
        return new SimpleReference(new ReferenceIdent(RELATION_ID, columnIdent), RowGranularity.DOC, DataTypes.STRING, 0, null);
    }

    @Test
    public void testConvertDocReference() throws Exception {
        // users._doc['name'] -> users.name
        Reference reference = stringRef("_doc.name");
        Reference newRef = (Reference) DocReferences.inverseSourceLookup(reference);
        assertThat(stringRef("name").ident()).isEqualTo(newRef.ident());

        // users._doc -> users._doc
        reference = stringRef("_doc");
        newRef = (Reference) DocReferences.inverseSourceLookup(reference);
        assertThat(stringRef("_doc").ident()).isEqualTo(newRef.ident());
    }

    @Test
    public void testDontConvertOtherReferences() throws Exception {
        Reference reference = stringRef("_raw");
        Reference newRef = (Reference) DocReferences.inverseSourceLookup(reference);
        assertThat(reference.ident()).isEqualTo(newRef.ident());

        reference = stringRef("_id");
        newRef = (Reference) DocReferences.inverseSourceLookup(reference);
        assertThat(reference.ident()).isEqualTo(newRef.ident());

        reference = stringRef("address.zip_code");
        newRef = (Reference) DocReferences.inverseSourceLookup(reference);
        assertThat(reference.ident()).isEqualTo(newRef.ident());
    }

    @Test
    public void test_apply_oid_to_references() {
        long[] oid = new long[1];

        var references = DocReferences.applyOid(
                List.of(stringRef("name"), stringRef("foo")),
                () -> ++oid[0]
        );
        assertThat(references.get(0).oid()).isEqualTo(1);
        assertThat(references.get(1).oid()).isEqualTo(2);
    }

    @Test
    public void test_index_source_references_has_oids() {
        var references = List.of(stringRef("name"), stringRef("first_name"));
        var referenceMap = references.stream()
                .collect(Collectors.toMap(Reference::column, reference -> reference));
        var indexReference = new IndexReference.Builder(new ReferenceIdent(RELATION_ID, new ColumnIdent("ft")))
                .sources(List.of("name", "first_name"))
                .build(referenceMap);
        long[] oid = new long[1];
        references = DocReferences.applyOid(
                Lists.concat(references, indexReference),
                () -> ++oid[0]
        );
        indexReference = (IndexReference) references.get(2);
        for (var ref : indexReference.columns()) {
            assertThat(ref.oid()).isGreaterThan(0);
        }
    }
}
