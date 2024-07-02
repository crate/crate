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

package io.crate.role;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.jetbrains.annotations.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class AlterRoleRequest extends AcknowledgedRequest<AlterRoleRequest> {

    private final String roleName;
    private final SecureHash secureHash;

    @Nullable
    private final JwtProperties jwtProperties;

    private final boolean resetPassword;

    private final boolean resetJwtProperties;


    public AlterRoleRequest(String roleName,
                            @Nullable SecureHash secureHash,
                            @Nullable JwtProperties jwtProperties,
                            boolean resetPassword,
                            boolean resetJwtProperties) {
        this.roleName = roleName;
        this.secureHash = secureHash;
        this.jwtProperties = jwtProperties;
        this.resetPassword = resetPassword;
        this.resetJwtProperties = resetJwtProperties;
    }

    public String roleName() {
        return roleName;
    }

    @Nullable
    public SecureHash secureHash() {
        return secureHash;
    }

    @Nullable
    public JwtProperties jwtProperties() {
        return jwtProperties;
    }

    public boolean resetPassword() {
        return resetPassword;
    }

    public boolean resetJwtProperties() {
        return resetJwtProperties;
    }

    public AlterRoleRequest(StreamInput in) throws IOException {
        super(in);
        roleName = in.readString();
        secureHash = in.readOptionalWriteable(SecureHash::readFrom);
        if (in.getVersion().onOrAfter(Version.V_5_7_0)) {
            this.jwtProperties = in.readOptionalWriteable(JwtProperties::readFrom);
            this.resetPassword = in.readBoolean();
            this.resetJwtProperties = in.readBoolean();
        } else {
            this.jwtProperties = null;
            this.resetPassword = false;
            this.resetJwtProperties = false;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(roleName);
        out.writeOptionalWriteable(secureHash);
        if (out.getVersion().onOrAfter(Version.V_5_7_0)) {
            out.writeOptionalWriteable(jwtProperties);
            out.writeBoolean(resetPassword);
            out.writeBoolean(resetJwtProperties);
        }
    }
}
