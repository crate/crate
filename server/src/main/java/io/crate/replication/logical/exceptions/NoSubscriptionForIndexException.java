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

package io.crate.replication.logical.exceptions;

import org.elasticsearch.index.Index;

import java.util.Locale;

/**
 * Marker exception raised when a subscription for a given index isn't found.
 * This can for example happen if a subscription is dropped while some replication resource is still running.
 */
public class NoSubscriptionForIndexException extends RuntimeException {

    private static final String MESSAGE = "No subscription found for index '%s'";

    public NoSubscriptionForIndexException(Index index) {
        super(String.format(Locale.ENGLISH, MESSAGE, index));
    }
}
