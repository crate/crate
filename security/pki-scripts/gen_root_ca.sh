#!/bin/bash
set -e
rm -rf ca certs* crl *.jks
CA_PASS=$1
TS_PASS=$2
mkdir -p ca/root-ca/private ca/root-ca/db crl certs
chmod 700 ca/root-ca/private

cp /dev/null ca/root-ca/db/root-ca.db
cp /dev/null ca/root-ca/db/root-ca.db.attr
echo 01 > ca/root-ca/db/root-ca.crt.srl
echo 01 > ca/root-ca/db/root-ca.crl.srl

openssl req -new \
    -config etc/root-ca.conf \
    -out ca/root-ca.csr \
    -keyout ca/root-ca/private/root-ca.key \
	-batch \
	-passout pass:$CA_PASS
	

openssl ca -selfsign \
    -config etc/root-ca.conf \
    -in ca/root-ca.csr \
    -out ca/root-ca.crt \
    -extensions root_ca_ext \
	-batch \
	-passin pass:$CA_PASS
	
echo Root CA generated
	
mkdir -p ca/signing-ca/private ca/signing-ca/db crl certs
chmod 700 ca/signing-ca/private

cp /dev/null ca/signing-ca/db/signing-ca.db
cp /dev/null ca/signing-ca/db/signing-ca.db.attr
echo 01 > ca/signing-ca/db/signing-ca.crt.srl
echo 01 > ca/signing-ca/db/signing-ca.crl.srl

openssl req -new \
    -config etc/signing-ca.conf \
    -out ca/signing-ca.csr \
    -keyout ca/signing-ca/private/signing-ca.key \
	-batch \
	-passout pass:$CA_PASS
	
openssl ca \
    -config etc/root-ca.conf \
    -in ca/signing-ca.csr \
    -out ca/signing-ca.crt \
    -extensions signing_ca_ext \
	-batch \
	-passin pass:$CA_PASS
	
echo Signing CA generated

#Useful stuff but not directly needed by this script
#openssl x509 -in ca/root-ca.crt -out ca/root-ca.der -outform DER
#openssl x509 -in ca/signing-ca.crt -out ca/signing-ca.der -outform DER
#openssl x509 -inform der -outform PEM -in ca/root-ca.der -out ca/root-ca.pem
#openssl x509 -inform der -outform PEM -in ca/signing-ca.der -out ca/signing-ca.pem
#cat ca/root-ca.pem ca/signing-ca.pem > ca/chain-ca.pem

"$JAVA_HOME/bin/keytool"  \
    -import  \
    -file ca/root-ca.crt  \
    -keystore truststore.jks   \
    -storepass $TS_PASS  \
    -noprompt -alias root-ca

"$JAVA_HOME/bin/keytool"  \
    -import \
    -file ca/signing-ca.crt  \
    -keystore truststore.jks   \
    -storepass $TS_PASS  \
    -noprompt -alias sig-ca