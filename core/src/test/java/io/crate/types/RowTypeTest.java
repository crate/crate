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

package io.crate.types;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class RowTypeTest {

    @Test
    public void testRowTypeIsConvertibleToSameRowType() throws Exception {
        RowType t1 = new RowType(Arrays.<DataType>asList(DataTypes.INTEGER, DataTypes.STRING));
        RowType t2 = new RowType(Arrays.<DataType>asList(DataTypes.INTEGER, DataTypes.STRING));

        assertThat(t1.isConvertableTo(t2), is(true));
    }

    @Test
    public void testRowTypeIsNotConvertibleToDifferentRowType() throws Exception {
        RowType t1 = new RowType(Arrays.<DataType>asList(DataTypes.INTEGER, DataTypes.STRING));
        RowType t2 = new RowType(Arrays.<DataType>asList(DataTypes.INTEGER, DataTypes.INTEGER));

        // if the types have the same number of sub-types and each of them is convertible, should this be possible?
        assertThat(t1.isConvertableTo(t2), is(false));
    }

    @Test
    public void testRowTypeIsConvertibleIfSingleInnerTypeMatches() throws Exception {
        RowType t1 = new RowType(Collections.<DataType>singletonList(DataTypes.INTEGER));
        assertThat(t1.isConvertableTo(DataTypes.INTEGER), is(true));
    }

    @Test
    public void testRowTypeIsNotConvertibleIfSingleInnerTypeIsConvertible() throws Exception {
        // require explicit casts
        // otherwise we'd have to support  something like to_string((select x))
        RowType t1 = new RowType(Collections.<DataType>singletonList(DataTypes.INTEGER));
        assertThat(t1.isConvertableTo(DataTypes.STRING), is(false));
    }

    @Test
    public void testRowTypeIsNotConvertibleIfSingleInnerTypeIsNot() throws Exception {
        RowType t1 = new RowType(Collections.<DataType>singletonList(DataTypes.INTEGER));
        assertThat(t1.isConvertableTo(DataTypes.OBJECT), is(false));
    }
}
