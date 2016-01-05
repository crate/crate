/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.reference.sys.node.fs;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import com.google.common.base.Predicate;
// import org.hyperic.sigar.FileSystem;

import javax.annotation.Nullable;

public class FileSystems {

    // TODO: FIX ME! sigar removed
    /* private final static IntSet SUPPORTED_FS_TYPES = IntHashSet.from(
            FileSystem.TYPE_LOCAL_DISK, FileSystem.TYPE_NETWORK, FileSystem.TYPE_RAM_DISK); */
    private final static IntSet SUPPORTED_FS_TYPES = null;

    /* public final static Predicate<FileSystem> SUPPORTED_FS_TYPE = new Predicate<FileSystem>() {
        @Override
        public boolean apply(@Nullable FileSystem input) {
            return (input != null && SUPPORTED_FS_TYPES.contains(input.getType()));
        }
    };*/
}
