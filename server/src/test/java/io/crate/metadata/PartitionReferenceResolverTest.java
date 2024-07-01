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

package io.crate.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.List;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;

public class PartitionReferenceResolverTest extends ESTestCase {

    @Test
    public void testClusterExpressionsNotAllowed() throws Exception {
        Reference refInfo = TestingHelpers.refInfo("foo.bar", DataTypes.STRING, RowGranularity.CLUSTER);
        PartitionReferenceResolver referenceResolver = new PartitionReferenceResolver(List.of());

        if (assertionsEnabled()) {
            try {
                referenceResolver.getImplementation(refInfo);
                fail("no assertion error thrown");
            } catch (AssertionError e) {
                assertThat(e.getMessage()).contains("granularity < PARTITION should have been resolved already");
            }
        } else {
            referenceResolver.getImplementation(refInfo);
        }


    }

    public static boolean assertionsEnabled() {
        boolean assertsEnabled = false;
        assert assertsEnabled = true; // Intentional side effect!!!
        return assertsEnabled;
    }
}
