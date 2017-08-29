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

import com.google.common.collect.ImmutableList;
import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class TableIdentTest extends CrateUnitTest {

    @Test
    public void testIndexName() throws Exception {
        TableIdent ti = new TableIdent(Schemas.DOC_SCHEMA_NAME, "t");
        assertThat(ti.indexName(), is("t"));
        ti = new TableIdent("s", "t");
        assertThat(ti.indexName(), is("s.t"));
    }

    @Test
    public void testFromIndexName() throws Exception {
        assertThat(TableIdent.fromIndexName("t"), is(new TableIdent(Schemas.DOC_SCHEMA_NAME, "t")));
        assertThat(TableIdent.fromIndexName("s.t"), is(new TableIdent("s", "t")));

        PartitionName pn = new PartitionName("s", "t", ImmutableList.of(new BytesRef("v1")));
        assertThat(TableIdent.fromIndexName(pn.asIndexName()), is(new TableIdent("s", "t")));

        pn = new PartitionName( "t", ImmutableList.of(new BytesRef("v1")));
        assertThat(TableIdent.fromIndexName(pn.asIndexName()), is(new TableIdent(Schemas.DOC_SCHEMA_NAME, "t")));
    }

    @Test
    public void testDefaultSchema() throws Exception {
        TableIdent ti = new TableIdent(Schemas.DOC_SCHEMA_NAME, "t");
        assertThat(ti.schema(), is("doc"));
        assertThat(ti, is(new TableIdent("doc", "t")));
    }

    @Test
    public void testFQN() throws Exception {
        TableIdent ti = new TableIdent(Schemas.DOC_SCHEMA_NAME, "t");
        assertThat(ti.fqn(), is("doc.t"));

        ti = new TableIdent("s", "t");
        assertThat(ti.fqn(), is("s.t"));
    }

    @Test
    public void testFqnFromIndexName() throws Exception {
        assertThat(TableIdent.fqnFromIndexName("t1"), is(Schemas.DOC_SCHEMA_NAME + ".t1"));
        assertThat(TableIdent.fqnFromIndexName("my_schema.t1"), is("my_schema.t1"));
        assertThat(TableIdent.fqnFromIndexName(".partitioned.t1.abc"), is(Schemas.DOC_SCHEMA_NAME + ".t1"));
        assertThat(TableIdent.fqnFromIndexName("my_schema..partitioned.t1.abc"), is("my_schema.t1"));
    }

    @Test
    public void testFqnFromIndexNameUnsupported3Parts() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid index name: my_schema.t1.foo");
        TableIdent.fqnFromIndexName("my_schema.t1.foo");
    }

    @Test
    public void testFqnFromIndexNameUnsupportedMoreThan5Parts() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid index name: my_schema..partitioned.t1.abc.foo");
        TableIdent.fqnFromIndexName("my_schema..partitioned.t1.abc.foo");
    }
}
