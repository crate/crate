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

package io.crate.metadata;

import io.crate.planner.RowGranularity;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ReferenceInfoTest {

    @Test
    public void testEquals() throws Exception {
        TableIdent tableIdent = new TableIdent("doc", "test");
        ReferenceIdent referenceIdent = new ReferenceIdent(tableIdent, "object_column");
        DataType dataType1 = new ArrayType(DataTypes.OBJECT);
        DataType dataType2 = new ArrayType(DataTypes.OBJECT);
        ReferenceInfo referenceInfo1 = new ReferenceInfo(referenceIdent, RowGranularity.DOC, dataType1);
        ReferenceInfo referenceInfo2 = new ReferenceInfo(referenceIdent, RowGranularity.DOC, dataType2);
        assertTrue(referenceInfo1.equals(referenceInfo2));
    }
}
