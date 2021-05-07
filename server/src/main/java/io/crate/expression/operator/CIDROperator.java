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

package io.crate.expression.operator;

import io.crate.common.collections.Tuple;
import io.crate.data.Input;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import org.elasticsearch.common.network.InetAddresses;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Locale;

public final class CIDROperator {

    public static final String CONTAINED_WITHIN = Operator.PREFIX + "<<";

    private static final int IPV4_ADDRESS_LEN = 4;

    public static void register(OperatorModule module) {
        module.register(
            Signature.scalar(
                CONTAINED_WITHIN,
                DataTypes.IP.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ),
            ContainedWithinOperator::new
        );
    }

    public static boolean containedWithin(String ipStr, String cidrStr) {
        if (null == ipStr || null == cidrStr) {
            throw new IllegalArgumentException("operands cannot be null");
        }
        if (false == cidrStr.contains("/") || ipStr.contains("/")) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "operands are incorrect, expected [ip, cidr], got [%s, %s]", ipStr, cidrStr));
        }
        try {
            BigInteger ip = new BigInteger(1, InetAddress.getByName(ipStr).getAddress());
            Tuple<BigInteger, BigInteger> cidr = extractRange(cidrStr);
            return cidr.v1().compareTo(ip) <= 0 && ip.compareTo(cidr.v2()) <= 0;
        } catch (UnknownHostException uhe) {
            throw new IllegalArgumentException(uhe);
        }
    }

    private static Tuple<BigInteger, BigInteger> extractRange(String cidr) {
        if (null == cidr || false == cidr.contains("/")) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH, "operand [%s] must conform with CIDR notation", cidr));
        }
        Tuple<InetAddress, Integer> tup = InetAddresses.parseCidr(cidr);
        InetAddress inetAddress = tup.v1();
        BigInteger base = new BigInteger(1, inetAddress.getAddress());
        BigInteger mask = createMask(inetAddress.getAddress().length, tup.v2());
        BigInteger start = base.and(mask);
        BigInteger end = start.add(mask.not());
        return new Tuple<>(start, end);
    }

    private static BigInteger createMask(int addressSizeInBytes, int prefixLength) {
        ByteBuffer maskBuffer = ByteBuffer.allocate(addressSizeInBytes);
        if (IPV4_ADDRESS_LEN == addressSizeInBytes) {
            maskBuffer = maskBuffer.putInt(-1);
        } else {
            maskBuffer = maskBuffer.putLong(-1L).putLong(-1L);
        }
        return new BigInteger(1, maskBuffer.array()).not().shiftRight(prefixLength);
    }

    public static class ContainedWithinOperator extends Scalar<Boolean, Object> {

        private final Signature signature;
        private final Signature boundSignature;

        public ContainedWithinOperator(Signature signature, Signature boundSignature) {
            this.signature = signature;
            this.boundSignature = boundSignature;
        }

        @Override
        public Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>[] args) {
            assert args.length == 2 : "number of args must be 2";
            String left = (String) args[0].value();
            if (null == left) {
                return null;
            }
            String right = (String) args[1].value();
            if (null == right) {
                return null;
            }
            return containedWithin(left, right);
        }

        @Override
        public Signature signature() {
            return signature;
        }

        @Override
        public Signature boundSignature() {
            return boundSignature;
        }
    }
}
