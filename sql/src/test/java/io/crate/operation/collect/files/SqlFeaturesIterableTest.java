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

package io.crate.operation.collect.files;

import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.*;

public class SqlFeaturesIterableTest {

    @Test
    public void testIterator() throws Exception {
        SqlFeaturesIterable featuresIterable = new SqlFeaturesIterable();
        Iterator iterator = featuresIterable.iterator();
        assertTrue(iterator.hasNext());
        SqlFeatureContext context = (SqlFeatureContext) iterator.next();
        assertEquals("B011", context.featureId);
        assertEquals("Embedded Ada", context.featureName);
        assertEquals("", context.subFeatureId);
        assertEquals("", context.subFeatureName);
        assertEquals(false, context.isSupported);
        assertNull(context.isVerifiedBy);
        assertNull(context.comments);
        int count = 1;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        assertEquals(672, count);
        assertFalse(iterator.hasNext());
    }
}
