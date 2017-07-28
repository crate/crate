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

package io.crate.operation.tablefunctions;

import org.junit.Test;

import java.util.Iterator;

public class GenerateSeriesFunctionTest extends AbstractTableFunctionsTest {

    private Iterator<String> getNumericString(int start, int step) {
        return new Iterator<String>() {
            Integer current = start;

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public String next() {
                String result = current.toString();
                current += step;
                return result;
            }
        };
    }

    private Iterator<String> getNumericString(long start, long step) {
        return new Iterator<String>() {
            Long current = start;

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public String next() {
                String result = current.toString();
                current += step;
                return result;
            }
        };
    }

    private Iterator<String> getNumericString(float start, float step) {
        return new Iterator<String>() {
            Float current = start;

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public String next() {
                String result = current.toString();
                current += step;
                return result;
            }
        };
    }

    private Iterator<String> getNumericString(double start, double step) {
        return new Iterator<String>() {
            Double current = start;

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public String next() {
                String result = current.toString();
                current += step;
                return result;
            }
        };
    }

    @Test
    public void testNormalCaseWithDefaultStep() throws Exception {

        Iterator i;
        i = getNumericString(0, 1);
        assertExecute("generate_series(0::integer,3::integer)", i.next()+"\n"+i.next()+"\n"+i.next()+"\n"+i.next()+"\n");

        i = getNumericString(0L, 1L);
        assertExecute("generate_series(0::long,3::long)", i.next()+"\n"+i.next()+"\n"+i.next()+"\n"+i.next()+"\n");

        i = getNumericString(0.7f, 1.0f);
        assertExecute("generate_series(0.7::float,3.7::float)", i.next()+"\n"+i.next()+"\n"+i.next()+"\n"+i.next()+"\n");

        i = getNumericString(0.7d, 1.0d);
        assertExecute("generate_series(0.7::double,3.7::double)", i.next()+"\n"+i.next()+"\n"+i.next()+"\n"+i.next()+"\n");
    }

    @Test
    public void testNormalCaseWithCustomStep() throws Exception {

        Iterator i;
        i = getNumericString(0, 3);
        assertExecute("generate_series(0::integer,8::integer,3::integer)", i.next()+"\n"+i.next()+"\n"+i.next()+"\n");

        i = getNumericString(0L, 3L);
        assertExecute("generate_series(0::long,8::long,3::long)", i.next()+"\n"+i.next()+"\n"+i.next()+"\n");

        i = getNumericString(0.7f, 3.1f);
        assertExecute("generate_series(0.7::float,8.7::float,3.1::float)", i.next()+"\n"+i.next()+"\n"+i.next()+"\n");

        i = getNumericString(0.7d, 3.1d);
        assertExecute("generate_series(0.7::double,8.7::double,3.1::double)", i.next()+"\n"+i.next()+"\n"+i.next()+"\n");
    }

    @Test
    public void testNegativeCase() throws Exception {

        Iterator i;
        i = getNumericString(8, -3);
        assertExecute("generate_series(8::integer,0::integer,-3::integer)", i.next()+"\n"+i.next()+"\n"+i.next()+"\n");

        i = getNumericString(8L, -3L);
        assertExecute("generate_series(8::long,0::long,-3::long)", i.next()+"\n"+i.next()+"\n"+i.next()+"\n");

        i = getNumericString(8.0f, -3.1f);
        assertExecute("generate_series(8::float,0.7::float,-3.1::float)", i.next()+"\n"+i.next()+"\n"+i.next()+"\n");

        i = getNumericString(8.0d, -3.1d);
        assertExecute("generate_series(8::double,0.7::double,-3.1::double)", i.next()+"\n"+i.next()+"\n"+i.next()+"\n");
    }
}
