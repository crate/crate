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

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.network.InetAddresses;

import io.crate.data.Input;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.types.EqQuery;

public final class CIDROperator {

    public static final String CONTAINED_WITHIN = Operator.PREFIX + "<<";

    private CIDROperator() {}

    public static void register(OperatorModule module) {
        module.register(
            Signature.scalar(
                CONTAINED_WITHIN,
                DataTypes.IP.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ).withFeature(Scalar.Feature.NULLABLE),
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

            InetAddresses.InetAddressPrefixLength cidr = InetAddresses.parseCidr(cidrStr);
            InetAddress[] bounds = obtainBounds(cidr.inetAddress(), cidr.prefixLen());
            BigInteger lower = new BigInteger(1, bounds[0].getAddress());
            BigInteger upper = new BigInteger(1, bounds[1].getAddress());

            return lower.compareTo(ip) <= 0 && ip.compareTo(upper) <= 0;
        } catch (UnknownHostException uhe) {
            throw new IllegalArgumentException(uhe);
        }
    }

    public static class ContainedWithinOperator extends Scalar<Boolean, Object> {

        public ContainedWithinOperator(Signature signature, BoundSignature boundSignature) {
            super(signature, boundSignature);
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
        public Query toQuery(Reference ref, Literal<?> literal) {
            String cidrStr = (String) literal.value();
            InetAddresses.InetAddressPrefixLength cidr = InetAddresses.parseCidr(cidrStr);
            InetAddress[] bounds = obtainBounds(cidr.inetAddress(), cidr.prefixLen());
            assert ref.valueType().id() == DataTypes.IP.id()
                : "In <ref> << <literal> the ref must have type IP due to function registration";
            EqQuery<? super String> eqQuery = DataTypes.IP.storageSupportSafe().eqQuery();
            if (eqQuery == null) {
                return null;
            }
            return eqQuery.rangeQuery(
                ref.storageIdent(),
                bounds[0].getHostAddress(),
                bounds[1].getHostAddress(),
                true,
                true,
                ref.hasDocValues(),
                ref.indexType() != IndexType.NONE);
        }
    }

    /**
     *  The logic is extracted from {@link org.apache.lucene.document.InetAddressPoint#newPrefixQuery(String, InetAddress, int)}
     */
    private static InetAddress[] obtainBounds(InetAddress value, int prefixLength) {
        if (prefixLength >= 0 && prefixLength <= 8 * value.getAddress().length) {
            byte[] lower = value.getAddress();
            byte[] upper = value.getAddress();

            for (int i = prefixLength; i < 8 * lower.length; ++i) {
                int m = 1 << 7 - (i & 7);
                lower[i >> 3] = (byte) (lower[i >> 3] & ~m);
                upper[i >> 3] = (byte) (upper[i >> 3] | m);
            }
            try {
                return new InetAddress[]{
                    InetAddress.getByAddress(lower),
                    InetAddress.getByAddress(upper)
                };
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException(e);
            }
        } else {
            throw new IllegalArgumentException("illegal prefixLength '" + prefixLength + "'. Must be 0-32 for IPv4 ranges, 0-128 for IPv6 ranges");
        }
    }
}
