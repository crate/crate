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

package io.crate.analyze;

import com.google.common.base.Optional;
import io.crate.analyze.repositories.RepositoryParamValidator;
import io.crate.analyze.repositories.RepositorySettingsModule;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.GenericProperty;
import io.crate.sql.tree.StringLiteral;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class RepositoryParamValidatorTest extends CrateUnitTest {

    private RepositoryParamValidator validator;

    @Before
    public void initValidator() throws Exception {
        validator = new ModulesBuilder()
            .add(new RepositorySettingsModule()).createInjector().getInstance(RepositoryParamValidator.class);
    }

    @Test
    public void testValidate() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid repository type \"invalid_type\"");
        validator.convertAndValidate("invalid_type", Optional.of(new GenericProperties()), ParameterContext.EMPTY);
    }

    @Test
    public void testRequiredTypeIsMissing() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "The following required parameters are missing to create a repository of type \"fs\": [location]");
        validator.convertAndValidate("fs", Optional.of(new GenericProperties()), ParameterContext.EMPTY);
    }

    @Test
    public void testInvalidSetting() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("setting 'yay' not supported");
        GenericProperties genericProperties = new GenericProperties();
        genericProperties.add(new GenericProperty("location", new StringLiteral("foo")));
        genericProperties.add(new GenericProperty("yay", new StringLiteral("invalid")));
        validator.convertAndValidate("fs", Optional.of(genericProperties), ParameterContext.EMPTY);
    }

    @Test
    public void testHdfsDynamicConfParam() throws Exception {
        GenericProperties genericProperties = new GenericProperties();
        genericProperties.add(new GenericProperty("path", new StringLiteral("/data")));
        genericProperties.add(new GenericProperty("conf.foobar", new StringLiteral("bar")));
        Settings settings = validator.convertAndValidate("hdfs", Optional.of(genericProperties), ParameterContext.EMPTY);
        assertThat(settings.get("conf.foobar"), is("bar"));
    }
}
