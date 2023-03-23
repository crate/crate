/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.testing;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.crate.planner.optimizer.Rule;

/**
 * Execute randomized "set optimizer_rule_name= false" as session setting.
 * Rules can be excluded from randomization e.g:
 * <pre>{@code
 * @UseRandomizedOptimizerRules(keep = MyOptimizerRule.class)
 * public void test_my_optimizer_rule() {
 *     ...
 * }
 * }</pre>
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
@Inherited
public @interface UseRandomizedOptimizerRules {

    // 0 -> disabled
    // 1 -> always enabled
    // close to 1 -> mostly enabled
    // close to 0 -> mostly disabled
    double value() default 0.5;

    Class<? extends Rule<?>>[] keep() default {};
}
