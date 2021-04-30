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

package io.crate.integrationtests;

import io.crate.testing.SQLResponse;
import org.junit.Test;

import java.util.Locale;

public class CrateSettingsIntegrationTest extends SQLIntegrationTestCase {

    @Test
    public void testAllSettingsAreSelectable() throws Exception {
        SQLResponse res = execute("select table_schema, table_name, column_name " +
                                  "from information_schema.columns " +
                                  "where column_name like 'settings%'");
        for (Object[] row : res.rows()) {
            logger.info("Selecting `{}` from `{}.{}`", row[2], row[0], row[1]);
            execute(String.format(Locale.ENGLISH, "select %s from %s.%s ", row[2], row[0], row[1]));
        }
    }
}
