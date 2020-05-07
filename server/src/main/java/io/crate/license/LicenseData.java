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

package io.crate.license;

import java.util.Objects;

public class LicenseData {

    private final long expiryDateInMs;
    private final String issuedTo;
    private final int maxNumberOfNodes;

    public LicenseData(long expiryDateInMs, String issuedTo, int maxNumberOfNodes) {
        this.expiryDateInMs = expiryDateInMs;
        this.issuedTo = issuedTo;
        this.maxNumberOfNodes = maxNumberOfNodes;
    }

    public long expiryDateInMs() {
        return expiryDateInMs;
    }

    public long millisToExpiration() {
        return expiryDateInMs - System.currentTimeMillis();
    }

    public String issuedTo() {
        return issuedTo;
    }

    public int maxNumberOfNodes() {
        return maxNumberOfNodes;
    }

    boolean isExpired() {
        return millisToExpiration() < 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LicenseData that = (LicenseData) o;
        return expiryDateInMs == that.expiryDateInMs &&
               maxNumberOfNodes == that.maxNumberOfNodes &&
               Objects.equals(issuedTo, that.issuedTo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expiryDateInMs, issuedTo, maxNumberOfNodes);
    }
}
