/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze.where;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.core.Is.is;

public class DocKeysTest extends CrateUnitTest {

    @Test
    public void testClusteredIsFirstInId() throws Exception {
        // if a the table is clustered and has a pk, the clustering value is put in front in the id computation
        List<List<Symbol>> pks = ImmutableList.of(
            ImmutableList.of(Literal.of(1), Literal.of("Ford"))
        );
        DocKeys docKeys = new DocKeys(pks, false, 1, null);
        DocKeys.DocKey key = docKeys.getOnlyKey();
        assertThat(key.routing(), is("Ford"));
        assertThat(key.id(), is("AgRGb3JkATE="));
    }

    @Test
    public void testPartitioned() throws Exception {
        List<List<Symbol>> pks = ImmutableList.of(
            ImmutableList.of(Literal.of(1), Literal.of("Ford"), Literal.of("test"))
        );
        DocKeys docKeys = new DocKeys(pks, false, 1, Collections.singletonList(2));
        DocKeys.DocKey key = docKeys.getOnlyKey();
        assertThat(key.routing(), is("Ford"));
        assertThat(key.partitionValues(),
            is(Optional.of(Collections.singletonList(new BytesRef("test")))));
    }

    @Test
    public void testIterator() {
        List<List<Symbol>> pks = ImmutableList.of(
            ImmutableList.of(Literal.of(1), Literal.of("Ford"), Literal.of("test")),
            ImmutableList.of(Literal.of(2), Literal.of("BMW"), Literal.of("test2"))
        );
        DocKeys docKeys = new DocKeys(pks, false, 1, Collections.singletonList(2));

        Iterator<DocKeys.DocKey> iterator = docKeys.iterator();
        DocKeys.DocKey docKey;

        docKey = iterator.next();
        assertThat(docKey.routing(), is("Ford"));
        assertThat(docKey.id(), is("AwRGb3JkATEEdGVzdA=="));
        assertThat(docKey.partitionValues(),
            is(Optional.of(Collections.singletonList(new BytesRef("test")))));
        assertThat(docKey.version(), is(Optional.empty()));

        docKey = iterator.next();
        assertThat(docKey.routing(), is("BMW"));
        assertThat(docKey.id(), is("AwNCTVcBMgV0ZXN0Mg=="));
        assertThat(docKey.partitionValues(),
            is(Optional.of(Collections.singletonList(new BytesRef("test2")))));
        assertThat(docKey.version(), is(Optional.empty()));
    }

    @Test
    public void testVersioned(){
        List<List<Symbol>> pks = ImmutableList.of(
            ImmutableList.of(Literal.of(1), Literal.of("Ford"), Literal.of(1L)),
            ImmutableList.of(Literal.of(2), Literal.of("BMW"), Literal.of(1L))
        );
        DocKeys docKeys = new DocKeys(pks, true, 1, Collections.singletonList(2));

        for (DocKeys.DocKey docKey : docKeys) {
            assertThat(docKey.version(), is(Optional.of(1L)));
        }
    }

}
