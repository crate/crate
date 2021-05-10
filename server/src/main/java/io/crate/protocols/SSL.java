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

package io.crate.protocols;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;

import javax.annotation.Nullable;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.SSLSession;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

public final class SSL {

    /**
     * Extract the common name from the subjectDN of a X509 Certificate
     */
    @Nullable
    public static String extractCN(Certificate certificate) {
        if (certificate instanceof X509Certificate) {
            return extractCN(((X509Certificate) certificate).getSubjectX500Principal().getName());
        }
        return null;
    }

    @Nullable
    public static SSLSession getSession(Channel channel) {
        SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
        if (sslHandler != null) {
            return sslHandler.engine().getSession();
        }
        return null;
    }

    private static String extractCN(String subjectDN) {
        /*
         * Get commonName using LdapName API
         * The DN of X509 certificates are in rfc2253 format. Ldap uses the same format.
         *
         * Doesn't use X500Name because it's internal API
         */
        try {
            LdapName ldapName = new LdapName(subjectDN);
            for (Rdn rdn : ldapName.getRdns()) {
                if ("CN".equalsIgnoreCase(rdn.getType())) {
                    return rdn.getValue().toString();
                }
            }
            throw new RuntimeException("Could not extract commonName from certificate subjectDN: " + subjectDN);
        } catch (InvalidNameException e) {
            throw new RuntimeException("Could not extract commonName from certificate", e);
        }
    }
}
