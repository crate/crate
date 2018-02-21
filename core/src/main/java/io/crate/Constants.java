/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate;

public class Constants {

    // Mapping Type that contains table definitions
    public static final String DEFAULT_MAPPING_TYPE = "default";

    // port ranges for HTTP and Transport
    public static final String HTTP_PORT_RANGE = "4200-4300";
    public static final String TRANSPORT_PORT_RANGE = "4300-4400";

    public static final int MAX_SHARD_MISSING_RETRIES = 10;

    public static final String KEYSTORE_DEFAULT_TYPE = "jks";
}
