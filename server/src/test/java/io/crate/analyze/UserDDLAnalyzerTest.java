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

package io.crate.analyze;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class UserDDLAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void initExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService).build();
    }

    @Test
    public void testCreateUserSimple() {
        AnalyzedCreateUser analysis = e.analyze("CREATE USER ROOT");
        assertThat(analysis.userName()).isEqualTo("root");
        analysis = e.analyze("CREATE USER \"ROOT\"");
        assertThat(analysis.userName()).isEqualTo("ROOT");
    }

    @Test
    public void testDropUserSimple() {
        AnalyzedDropUser analysis = e.analyze("DROP USER ROOT");
        assertThat(analysis.userName()).isEqualTo("root");
        analysis = e.analyze("DROP USER \"ROOT\"");
        assertThat(analysis.userName()).isEqualTo("ROOT");
    }

    @Test
    public void testDropUserIfExists() {
        AnalyzedDropUser analysis = e.analyze("DROP USER IF EXISTS ROOT");
        assertThat(analysis.userName()).isEqualTo("root");
        assertThat(analysis.ifExists()).isEqualTo(true);
    }

    @Test
    public void testCreateUserWithPassword() throws Exception {
        AnalyzedCreateUser analysis = e.analyze("CREATE USER ROOT WITH (PASSWORD = 'ROOT')");
        assertThat(analysis.userName()).isEqualTo("root");
        assertThat(analysis.properties().get("password")).isLiteral("ROOT");
    }

    @Test
    public void testCreateUserWithPasswordIsStringLiteral() throws Exception {
        assertThatThrownBy(() -> e.analyze("CREATE USER ROO WITH (PASSWORD = NO_STRING)"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Columns cannot be used in this context. " +
                        "Maybe you wanted to use a string literal which requires single quotes: 'no_string'");
    }

    @Test
    public void testAlterUserWithPassword() throws Exception {
        AnalyzedAlterUser analysis = e.analyze("ALTER USER ROOT SET (PASSWORD = 'ROOT')");
        assertThat(analysis.userName()).isEqualTo("root");
        assertThat(analysis.properties().get("password")).isLiteral("ROOT");
    }

    @Test
    public void testAlterUserResetPassword() throws Exception {
        AnalyzedAlterUser analysis = e.analyze("ALTER USER ROOT SET (PASSWORD = NULL)");
        assertThat(analysis.userName()).isEqualTo("root");
        assertThat(analysis.properties().get("password")).isLiteral(null, DataTypes.UNDEFINED);
    }
}
