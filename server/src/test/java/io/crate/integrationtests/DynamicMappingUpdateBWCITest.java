/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.integrationtests;

import static io.crate.testing.Asserts.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

@IntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0)
public class DynamicMappingUpdateBWCITest extends IntegTestCase {

    /// Ensures that we don't run into an NPE if an object reference is created with an undefined inner type
    /// and there are no child references created. This could happen on versions prior to 6.1.3.
    @Test
    public void test_6_1_object_reference_with_undefined_inner_type_and_no_child_reference() throws Exception {
        startUpNodeWithDataDir("/indices/data_home/cratedata-6.1.2-dynamic-object-null-element.zip");
        // cr> CREATE TABLE t1 (obj OBJECT(DYNAMIC));
        // cr> INSERT INTO t1 (obj) VALUES ({a={b=NULL}});

        execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 't1' ORDER BY column_name");
        assertThat(response).hasRows(
            "obj| object",
            "obj['a']| object"
        );

        // The following insert should not fail with an NPE
        execute("INSERT INTO doc.t1 (obj) VALUES ({a={c=1}})");

        execute("REFRESH TABLE doc.t1");
        assertThat(execute("SELECT * FROM doc.t1 ORDER BY obj['a']['c'] NULLS FIRST")).hasRows(
            "{a={b=NULL}}",
            "{a={c=1}}"
        );
    }
}
