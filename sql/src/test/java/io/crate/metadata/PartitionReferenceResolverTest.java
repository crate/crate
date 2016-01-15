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

import com.google.common.collect.ImmutableList;
import io.crate.operation.reference.partitioned.PartitionExpression;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PartitionReferenceResolverTest extends CrateUnitTest {

    @Test
    public void testClusterExpressionsNotAllowed() throws Exception {
        NestedReferenceResolver fallbackRefResolver = mock(NestedReferenceResolver.class);
        ReferenceInfo refInfo = TestingHelpers.refInfo("foo.bar", DataTypes.STRING, RowGranularity.CLUSTER);
        when(fallbackRefResolver.getImplementation(refInfo)).thenReturn(new ReferenceImplementation() {
            @Override
            public Object value() {
                return null;
            }

            @Override
            public ReferenceImplementation getChildImplementation(String name) {
                return null;
            }
        });
        PartitionReferenceResolver referenceResolver = new PartitionReferenceResolver(
                fallbackRefResolver,
                ImmutableList.<PartitionExpression>of()
        );

        if (assertionsEnabled()) {
            try {
                referenceResolver.getImplementation(refInfo);
                fail("no assertion error thrown");
            } catch (AssertionError e) {
                assertThat(e.getMessage(), is("granularity < PARTITION should have been resolved already"));
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