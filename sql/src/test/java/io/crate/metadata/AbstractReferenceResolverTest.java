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

package io.crate.metadata;

import io.crate.types.DataTypes;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class AbstractReferenceResolverTest {

    static final TableIdent USERS_TI = new TableIdent(null, "users");

    static class RefResolver extends AbstractReferenceResolver {

        public RefResolver() {
            ReferenceIdent ident = new ReferenceIdent(USERS_TI, new ColumnIdent("obj"));
            implementations.put(ident, mock(ReferenceImplementation.class));
        }
    }

    @Test
    public void testGetImplementation() throws Exception {
        RefResolver refResolver = new RefResolver();
        ReferenceImplementation implementation = refResolver.getImplementation(new Reference(
            new ReferenceIdent(USERS_TI, new ColumnIdent("obj", Arrays.asList("x", "z"))), RowGranularity.DOC, DataTypes.STRING));

        assertThat(implementation, Matchers.nullValue());
    }
}
