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

package io.crate.execution.engine.collect.files;

import org.junit.Test;

import java.util.stream.StreamSupport;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class FilesIterablesTest {

    @Test
    public void testSqlFeatureIterable() throws Exception {
        Iterable<SqlFeatureContext> sqlFeatureContextIterable = SqlFeatures.loadFeatures();

        var iterator = sqlFeatureContextIterable.iterator();
        assertTrue(iterator.hasNext());
        SqlFeatureContext context = iterator.next();
        assertEquals("B011", context.featureId);
        assertEquals("Embedded Ada", context.featureName);
        assertEquals("", context.subFeatureId);
        assertEquals("", context.subFeatureName);
        assertEquals(false, context.isSupported);
        assertNull(context.isVerifiedBy);
        assertNull(context.comments);

        assertThat(
            StreamSupport.stream(sqlFeatureContextIterable.spliterator(), false).count(),
            is(672L)
        );
    }

    @Test
    public void testSummitsIterable() throws Exception {
        SummitsIterable summitsIterable = new SummitsIterable();
        assertThat(
            StreamSupport.stream(summitsIterable.spliterator(), false).count(),
            is(1605L)
        );
    }
}
