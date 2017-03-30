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

package io.crate.bootstrap;


import java.nio.file.Path;
import java.util.Map;

/**
 * Copy of ES's {@link org.elasticsearch.bootstrap.BootstrapException}, it's not accessible from Crate's namespace.
 *
 * Wrapper exception for checked exceptions thrown during the bootstrap process. Methods invoked
 * during bootstrap should explicitly declare the checked exceptions that they can throw, rather
 * than declaring the top-level checked exception {@link Exception}. This exception exists to wrap
 * these checked exceptions so that {@link org.elasticsearch.bootstrap.BootstrapProxy#init(boolean, Path, boolean, Map)} does not have to
 * declare all of these checked exceptions.
 */
public class BootstrapException extends Exception {

    /**
     * Wraps an existing exception.
     *
     * @param cause the underlying cause of bootstrap failing
     */
    public BootstrapException(final Exception cause) {
        super(cause);
    }
}
