/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.operation.reference.sys.check;

import com.google.common.base.Supplier;
import org.elasticsearch.common.inject.Singleton;

import java.util.HashSet;
import java.util.Set;

@Singleton
public class SysChecker implements Supplier<Iterable<?>> {

    private final Set<? extends SysCheck> sysChecks;

    public SysChecker(Set<? extends SysCheck> sysChecks) {
        assert idsAreUnique(sysChecks) : "IDs of SysChecks are not unique";
        this.sysChecks = sysChecks;
    }

    private void check() {
        for (SysCheck sysCheck : sysChecks) {
            sysCheck.validate();
        }
    }

    @Override
    public Iterable<?> get() {
        check();
        return sysChecks;
    }

    private boolean idsAreUnique(Set<? extends SysCheck> sysChecks) {
        Set<Integer> ids = new HashSet<>();
        for (SysCheck sysCheck : sysChecks) {
            if (!ids.add(sysCheck.id())) {
                return false;
            }
        }
        return true;
    }
}

