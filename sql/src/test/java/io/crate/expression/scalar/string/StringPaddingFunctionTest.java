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

package io.crate.expression.scalar.string;

import io.crate.expression.scalar.AbstractScalarFunctionsTest;
import io.crate.expression.symbol.Literal;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class StringPaddingFunctionTest extends AbstractScalarFunctionsTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void test_evaluate_lpad_function() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("len argument exceeds predefined limit of 1000");
        assertEvaluate("lpad('yes', 2000000, 'yes')", null);
        assertEvaluate("lpad('yes', a, 'yes')", null, Literal.of(2000000));

        assertEvaluate("lpad(null, 5, '')", null);
        assertEvaluate("lpad('', null, '')", null);
        assertEvaluate("lpad('', 5, null)", null);
        assertEvaluate("lpad(name, 5, '')", null, Literal.of((String) null));
        assertEvaluate("lpad('', a, '')", null, Literal.of((Integer) null));
        assertEvaluate("lpad('', 5, name)", null, Literal.of((String) null));

        assertEvaluate("lpad('', 5, '')", "");
        assertEvaluate("lpad('yes', 0, 'yes')", "");
        assertEvaluate("lpad('yes', -1, 'yes')", "");
        assertEvaluate("lpad(name, 5, name)", "", Literal.of(""));
        assertEvaluate("lpad('yes', a, 'yes')", "", Literal.of(0));
        assertEvaluate("lpad('yes', a, 'yes')", "", Literal.of(1));

        assertEvaluate("lpad('yes', 5, '')", "yes");
        assertEvaluate("lpad('yes', 5, name)", "yes", Literal.of(""));

        assertEvaluate("lpad('', 5, 'yes')", "yesye");
        assertEvaluate("lpad('', 5, name)", "yesye", Literal.of("yes"));
        assertEvaluate("lpad('yes', 3, 'yes')", "yes");
        assertEvaluate("lpad('yes', 3, name)", "yes", Literal.of("yes"));
        assertEvaluate("lpad('yes', 1, 'yes')", "y");
        assertEvaluate("lpad('yes', 1, name)", "y", Literal.of("yes"));

        assertEvaluate("lpad('yes', 1)", "y");
        assertEvaluate("lpad('yes', 3)", "yes");
        assertEvaluate("lpad('yes', 1)", "y");
        assertEvaluate("lpad('yes', 5)", "yes  ");

        assertEvaluate("lpad(' I like CrateDB!!', 41, 'yes! ')", " yes! yes! yes! yes! yes! I like CrateDB!!");
        assertEvaluate("lpad(' I like CrateDB!!', 41, name)",
                       " yes! yes! yes! yes! yes! I like CrateDB!!",
                       Literal.of("yes! "));
        assertEvaluate("lpad(name, 41, 'yes! ')",
                       " yes! yes! yes! yes! yes! I like CrateDB!!",
                       Literal.of(" I like CrateDB!!"));
        assertEvaluate("lpad(' I like CrateDB!!', a, 'yes! ')",
                       " yes! yes! yes! yes! yes! I like CrateDB!!",
                       Literal.of(41));
    }

    @Test
    public void test_evaluate_rpad_function() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("len argument exceeds predefined limit of 1000");
        assertEvaluate("rpad('yes', 2000000, 'yes')", null);
        assertEvaluate("rpad('yes', a, 'yes')", null, Literal.of(2000000));

        assertEvaluate("rpad(null, 5, '')", null);
        assertEvaluate("rpad('', null, '')", null);
        assertEvaluate("rpad('', 5, null)", null);
        assertEvaluate("rpad(name, 5, '')", null, Literal.of((String) null));
        assertEvaluate("rpad('', a, '')", null, Literal.of((Integer) null));
        assertEvaluate("rpad('', 5, name)", null, Literal.of((String) null));

        assertEvaluate("rpad('', 5, '')", "");
        assertEvaluate("rpad('yes', 0, 'yes')", "");
        assertEvaluate("rpad('yes', -1, 'yes')", "");
        assertEvaluate("rpad(name, 5, name)", "", Literal.of(""));
        assertEvaluate("rpad('yes', a, 'yes')", "", Literal.of(0));
        assertEvaluate("rpad('yes', a, 'yes')", "", Literal.of(1));

        assertEvaluate("rpad('yes', 5, '')", "yes");
        assertEvaluate("rpad('yes', 5, name)", "yes", Literal.of(""));

        assertEvaluate("rpad('', 5, 'yes')", "yesye");
        assertEvaluate("rpad('', 5, name)", "yesye", Literal.of("yes"));
        assertEvaluate("rpad('yes', 3, 'yes')", "yes");
        assertEvaluate("rpad('yes', 3, name)", "yes", Literal.of("yes"));
        assertEvaluate("rpad('yes', 1, 'yes')", "y");
        assertEvaluate("rpad('yes', 1, name)", "y", Literal.of("yes"));

        assertEvaluate("rpad('yes', 1)", "y");
        assertEvaluate("rpad('yes', 3)", "yes");
        assertEvaluate("rpad('yes', 1)", "y");
        assertEvaluate("rpad('yes', 5)", "  yes");

        assertEvaluate("rpad('Do you like Crate?', 38, ' yes!')", "Do you like Crate? yes! yes! yes! yes!");
        assertEvaluate("rpad('Do you like Crate?', 38, name)",
                       "Do you like Crate? yes! yes! yes! yes!",
                       Literal.of(" yes!"));
        assertEvaluate("rpad(name, 38, ' yes!')",
                       "Do you like Crate? yes! yes! yes! yes!",
                       Literal.of("Do you like Crate?"));
        assertEvaluate("rpad('Do you like Crate?', a, ' yes!')",
                       "Do you like Crate? yes! yes! yes! yes!",
                       Literal.of(38));
    }
}
