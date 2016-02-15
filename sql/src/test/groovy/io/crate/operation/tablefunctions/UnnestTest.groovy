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

package io.crate.operation.tablefunctions

import com.carrotsearch.randomizedtesting.RandomizedTest
import io.crate.analyze.symbol.Literal
import io.crate.core.collections.Bucket
import io.crate.testing.TestingHelpers
import io.crate.types.ArrayType
import io.crate.types.DataType
import io.crate.types.DataTypes
import org.junit.Test

import static io.crate.analyze.symbol.Literal.newLiteral

class UnnestTest extends RandomizedTest {

    @Test
    void testExecute() {
        Unnest unnest = new Unnest();
        DataType stringArray = new ArrayType(DataTypes.STRING);
        DataType longArray = new ArrayType(DataTypes.LONG);

        Literal numbers = newLiteral(longArray, $(1L, 2L))
        Literal names = newLiteral(stringArray, $("Marvin", "Trillian"))
        Bucket bucket = unnest.execute([numbers, names])

        assert TestingHelpers.printedTable(bucket) == "1| Marvin\n" +
                "2| Trillian\n"
    }
}
