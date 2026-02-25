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

package io.crate.common.concurrent;


public interface Killable {

    /**
     * <p>
     * Interrupt the operation, increasing the likelihood that it will terminate early with an error.
     * This method can be called concurrently from a different thread while an operation is running.
     * </p>
     * Implementations must:
     * <ul>
     *
     *  <li>terminate if they're otherwise waiting for input/data</li>
     *  <li>terminate expensive and long running operations within a reasonable amount of time (&lt; 2sec)</li>
     *
     * </ul>
     *
     * Fast operations which are already running and can complete without further input may ignore the kill.
     * Operations which have already completed may also ignore the kill.
     *
     * @param throwable the reason for the interruption or null if there is none.
     */
    void kill(Throwable throwable);
}
