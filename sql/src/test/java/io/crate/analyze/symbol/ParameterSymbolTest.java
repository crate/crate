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

package io.crate.analyze.symbol;

import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class ParameterSymbolTest extends CrateUnitTest {

    @Test
    public void testSerialization() throws Exception {
        ParameterSymbol ps1 = new ParameterSymbol(2, DataTypes.INTEGER);

        BytesStreamOutput out = new BytesStreamOutput();
        ps1.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ParameterSymbol ps2 = new ParameterSymbol(in);

        assertThat(ps2.index(), is(ps1.index()));
        assertThat(ps2.valueType(), is(ps1.valueType()));
    }
}
