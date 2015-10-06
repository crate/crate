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

package io.crate.types;

import com.spatial4j.core.shape.Shape;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class GeoShapeTypeTest extends CrateUnitTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private GeoShapeType type = GeoShapeType.INSTANCE;

    @Test
    public void testStreamer() throws Exception {
        Shape value = type.value("POINT (10 10)");

        BytesStreamOutput out = new BytesStreamOutput();
        type.writeValueTo(out, value);
        BytesStreamInput in = new BytesStreamInput(out.bytes());
        Shape streamedShape = type.readValueFrom(in);

        assertThat(streamedShape, equalTo(value));
    }

    @Test
    public void testCompareValueTo() throws Exception {
        Shape val1 = type.value("POLYGON ( (0 0, 20 0, 20 20, 0 20, 0 0 ))");
        Shape val2 = type.value("POINT (10 10)");

        assertThat(type.compareValueTo(val1, val2), is(1));
        assertThat(type.compareValueTo(val2, val1), is(-1));
        assertThat(type.compareValueTo(val2, val2), is(0));
    }

    @Test
    public void testInvalidStringValueCausesIllegalArgumentException() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot convert \"foobar\" to geo_shape");
        type.value("foobar");
    }
    @Test
    public void testInvalidTypeCausesIllegalArgumentException() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot convert \"200\" to geo_shape");
        type.value(200);
    }
}