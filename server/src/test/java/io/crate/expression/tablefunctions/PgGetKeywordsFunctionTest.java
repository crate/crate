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

package io.crate.expression.tablefunctions;

import io.crate.data.Row;
import io.crate.data.RowN;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class PgGetKeywordsFunctionTest extends AbstractTableFunctionsTest {

    @Test
    public void test_pg_get_keywords() {
        var it = execute("pg_catalog.pg_get_keywords()").iterator();
        List<Row> rows = new ArrayList<>();
        while (it.hasNext()) {
            rows.add(new RowN(it.next().materialize()));
        }
        rows.sort(Comparator.comparing(x -> ((String) x.get(0))));
        assertThat(rows.size(), is(253));
        Row row = rows.get(0);

        assertThat(row.get(0), is("add"));
        assertThat(row.get(1), is("R"));
        assertThat(row.get(2), is("reserved"));
    }
}
