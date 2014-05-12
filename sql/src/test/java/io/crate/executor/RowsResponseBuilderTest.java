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

package io.crate.executor;

import com.google.common.base.Joiner;
import io.crate.action.sql.SQLResponse;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class RowsResponseBuilderTest {

    static Joiner commaJoiner = Joiner.on(", ");

    @Test
    public void testBuildResponseSetString() throws Exception {
        boolean convertBytesRef = true;
        RowsResponseBuilder rrb = new RowsResponseBuilder(convertBytesRef);

        String[] outputNames = new String[] { "col" };
        DataType[] dataTypes = new DataType[] { new SetType(DataTypes.STRING) };
        Object[][] rows = new Object[1][1];
        Set<BytesRef> refs = new HashSet<>(
                Arrays.asList(new BytesRef("foo"), new BytesRef("bar")));

        rows[0][0] = refs;
        SQLResponse response = rrb.buildResponse(dataTypes, outputNames, rows, 0L);
        assertThat(commaJoiner.join((String[])response.rows()[0][0]), is("foo, bar"));
    }

    @Test
    public void testBuildResponseArrayString() throws Exception {
        boolean convertBytesRef = true;
        RowsResponseBuilder rrb = new RowsResponseBuilder(convertBytesRef);

        String[] outputNames = new String[] { "col" };
        DataType[] dataTypes = new DataType[] { new ArrayType(DataTypes.STRING) };
        Object[][] rows = new Object[1][1];
        BytesRef[] refs = new BytesRef[] { new BytesRef("foo"), new BytesRef("bar") };

        rows[0][0] = refs;
        SQLResponse response = rrb.buildResponse(dataTypes, outputNames, rows, 0L);
        assertThat(commaJoiner.join((String[])response.rows()[0][0]), is("foo, bar"));
    }

}
