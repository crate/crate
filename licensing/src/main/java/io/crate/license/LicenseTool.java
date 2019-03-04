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

import io.crate.types.DataTypes;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;

import javax.crypto.Cipher;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;

public class LicenseTool {

    public static void main(String[] args) throws Exception {
        LogConfigurator.configureWithoutConfig(Settings.EMPTY);
        OptionParser parser = new OptionParser();
        ArgumentAcceptingOptionSpec<String> keyArg = parser.accepts("key", "Path to private key")
            .withRequiredArg()
            .ofType(String.class);
        ArgumentAcceptingOptionSpec<String> expirationDateArg = parser.accepts("expiration-date")
            .withRequiredArg()
            .ofType(String.class);
        ArgumentAcceptingOptionSpec<String> issuedToArg = parser.accepts("issued-to")
            .withRequiredArg()
            .ofType(String.class);
        ArgumentAcceptingOptionSpec<Integer> maxNodesArg = parser.accepts("max-nodes")
            .withRequiredArg()
            .ofType(Integer.class);

        String keyPath;
        long expirationDate;
        String issuedTo;
        int maxNodes;
        try {
            OptionSet optionSet = parser.parse(args);
            keyPath = keyArg.value(optionSet);
            expirationDate = DataTypes.TIMESTAMP.value(expirationDateArg.value(optionSet));
            issuedTo = issuedToArg.value(optionSet);
            maxNodes = maxNodesArg.value(optionSet);
            if (maxNodes <= 0) throw new IllegalArgumentException("max-nodes needs to be a positive number");
        } catch (Exception e) {
            parser.printHelpOn(System.err);
            System.exit(1);
            return;
        }

        byte[] keyBytes = Files.readAllBytes(Paths.get(keyPath));
        PrivateKey privateKey = KeyFactory.getInstance(CryptoUtils.RSA_CIPHER_ALGORITHM)
            .generatePrivate(new PKCS8EncodedKeySpec(keyBytes));

        byte[] licenseData = new DecryptedLicenseData(expirationDate, issuedTo, maxNodes).formatLicenseData();
        byte[] encryptedLicenseData = CryptoUtils.crypto(
                CryptoUtils.RSA_CIPHER_ALGORITHM,  
                Cipher.ENCRYPT_MODE,
                privateKey,
                licenseData);
        LicenseKey licenseKey = LicenseKey.createLicenseKey(
            LicenseKey.LicenseType.ENTERPRISE,
            2,
            encryptedLicenseData);
        System.out.println(licenseKey.licenseKey());
    }
}
