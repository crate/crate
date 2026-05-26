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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.repositories.RepositoryParamValidator;
import io.crate.analyze.repositories.TypeSettings;
import io.crate.expression.symbol.Literal;
import io.crate.sql.tree.GenericProperties;

public class RepositoryParamValidatorTest extends ESTestCase {

    private RepositoryParamValidator validator;

    @Before
    public void initValidator() {
        validator = new RepositoryParamValidator(
            Map.of("fs", new TypeSettings(FsRepository.mandatorySettings(), FsRepository.optionalSettings()))
        );
    }

    @Test
    public void test_validate_invalid_type() {
        assertThatThrownBy(() -> validator.validate("invalid_type", Settings.EMPTY))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid repository type \"invalid_type\"");
    }

    @Test
    public void test_validate_required_param_missing() {
        assertThatThrownBy(() -> validator.validate("fs", Settings.EMPTY))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The following required parameters are missing to create a repository of type \"fs\": [location]");
    }

    @Test
    public void test_validate_unsupported_setting() {
        assertThatThrownBy(() -> validator.validate("fs", Settings.builder().put("foo", "bar").build()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Setting 'foo' is not supported");
    }

    @Test
    public void test_validate_supported_only_with_unsupported_setting() {
        assertThatThrownBy(() -> validator.validateSupportedOnly("fs", new GenericProperties<>(Map.of("foo", Literal.of("bar")))))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Setting 'foo' is not supported");
    }

    @Test
    public void test_validate_supported_only_with_mandatory_only() {
        validator.validateSupportedOnly(
            "fs",
            new GenericProperties<>(Map.of("location", Literal.of("/tmp/something")))
        );
    }

    @Test
    public void test_validate_supported_only_with_mandatory_and_optional() {
        validator.validateSupportedOnly(
            "fs",
            new GenericProperties<>(Map.of(
                "location", Literal.of("/tmp/something"),
                "compress", Literal.of(false)
            ))
        );
    }

    @Test
    public void test_validate_can_reset_optional() {
        // single
        validator.validateCanReset("fs", List.of("compress"));
        // multiple
        validator.validateCanReset("fs", FsRepository.optionalSettings().stream().map(Setting::getKey).toList());
        // empty inputs
        validator.validateCanReset("fs", List.of());
    }

    @Test
    public void test_validate_can_reset_fails_with_required() {
        // single required
        assertThatThrownBy(() -> validator.validateCanReset("fs", List.of("location")))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The following required parameters are required for type \"fs\" and cannot be reset: [location]");

        // required mixed with optional
        assertThatThrownBy(() -> validator.validateCanReset("fs", List.of("compress", "location")))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The following required parameters are required for type \"fs\" and cannot be reset: [location]");
    }
}
