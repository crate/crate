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

package io.crate.gradle;

import java.util.Locale;

public final class OS {

    private final String osName;
    private final String arch;

    private OS(String osName, String arch) {
        this.osName = osName;
        this.arch = arch;
    }

    private static OS mac(String arch) {
        return new OS("mac", arch);
    }

    private static OS linux(String arch) {
        return new OS("linux", arch);
    }

    private static OS windows(String arch) {
        return new OS("windows", arch);
    }

    public String osName() {
        return osName;
    }

    public String arch() {
        return arch;
    }

    @SuppressWarnings("unused")
    public static OS current() {
        String arch = System.getProperty("os.arch", "");
        if ("amd64".equals(arch)) {
            arch = "x64";
        }
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (os.startsWith("windows")) {
            return windows(arch);
        }
        if (os.startsWith("linux")) {
            return linux(arch);
        }
        if (os.startsWith("mac") || os.startsWith("darwin")) {
            return mac(arch);
        }
        throw new IllegalStateException("Can't determine OS from: " + os);
    }
}
