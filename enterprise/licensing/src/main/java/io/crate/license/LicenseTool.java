/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
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
            expirationDate = DataTypes.TIMESTAMPZ.value(expirationDateArg.value(optionSet));
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

        byte[] licenseData = LicenseConverter.toJson(new LicenseData(expirationDate, issuedTo, maxNodes));
        byte[] encryptedLicenseData = CryptoUtils.crypto(
                CryptoUtils.RSA_CIPHER_ALGORITHM,
                Cipher.ENCRYPT_MODE,
                privateKey,
                licenseData);
        LicenseKey licenseKey = LicenseKey.encode(License.Type.ENTERPRISE, 2, encryptedLicenseData);
        System.out.println(licenseKey.licenseKey());
    }
}
