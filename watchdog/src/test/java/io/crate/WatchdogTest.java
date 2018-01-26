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

package io.crate;

import io.crate.action.sql.SQLOperations;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WatchdogTest extends CrateUnitTest {

    @Test
    public void testCanServeSqlRequest() {
        SQLOperations sqlOperations = mock(SQLOperations.class);
        when(sqlOperations.isEnabled()).thenReturn(true);
        Watchdog watchdog = new Watchdog(sqlOperations);
        assertThat(watchdog.canServeSQLRequests(), is(true));
    }

    @Test
    public void testCannotServeSqlRequestAsSQLIsDisabled() {
        SQLOperations sqlOperations = mock(SQLOperations.class);
        when(sqlOperations.isEnabled()).thenReturn(false);
        Watchdog watchdog = new Watchdog(sqlOperations);
        assertThat(watchdog.canServeSQLRequests(), is(false));
    }
}
