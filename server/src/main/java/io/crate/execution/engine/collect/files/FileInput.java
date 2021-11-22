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

package io.crate.execution.engine.collect.files;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.function.Predicate;

public interface FileInput {

    /**
     * this method returns all files that are found within fileUri
     *
     * @param fileUri      uri that points to a directory
     *                     (and may optionally contain a "file hint" - which is the part after the last slash.)
     *                     a concrete implementation may ignore the file hint.
     * @param uriPredicate predicate that a concrete implementation of FileInput must use to pre-filter the returned uris
     * @param protocolSetting
     * @return a list of Uris
     * @throws IOException
     */
    List<URI> listUris(URI fileUri, URI preGlobUri, Predicate<URI> uriPredicate, @Nullable String protocolSetting) throws IOException;

    InputStream getStream(URI uri, @Nullable String protocolSetting) throws IOException;

    boolean sharedStorageDefault();
}
