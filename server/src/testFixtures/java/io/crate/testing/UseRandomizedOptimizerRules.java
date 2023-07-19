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
 * Randomize optimizer rules via {@code set <optimizer_rule_name> = false}.
 *
 * <p>
 * To exclude rules from randomization:
 * </p>
 *
 * <pre>
 * @UseRandomizedOptimizerRules(alwaysKeep = MyOptimizerRule.class)
 * </pre>
 *
 * <p>
 * To disable randomization:
 * <pre>
 * @UseRandomizedOptimizerRules(value = 0)
 * </pre>
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
@Inherited
public @interface UseRandomizedOptimizerRules {

    /**
     * <ul>
     *  <li>0 -> disabled</li>
     *  <li>close to 0 - mostly disabled</li>
     *  <li>1 -> enabled</li>
     *  <li>close to 1 -> mostly enabled</li>
     * </ul>
     **/
    double value() default 0.5;

    /**
     * <ul>
     *  <li>-1  -> disable random number of optimizer rules</li>
     *  <li>0.1 -> disable randomly 10 % of the optimizer rules</li>
     *  <li>1   -> disable all the optimizer rules</li>
     * </ul>
     **/
    double disablePercentage() default -1;

    Class<? extends Rule<?>>[] alwaysKeep() default {};
}
