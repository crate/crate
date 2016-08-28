
/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.scalar.string;

import static io.crate.testing.TestingHelpers.isLiteral;

import org.junit.Test;

import io.crate.analyze.symbol.Literal;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataTypes;

public class Sha1FunctionTest extends AbstractScalarFunctionsTest {
	
	private String testLongText = "sha1('Dawn Capital is an early stage VC firm run by entrepreneurs and investing in startups across Europe. "
    		+ "It supports SaaS and Fintech companies that develop world leading technology to improve business value chains and productivity. "
    		+ "Dawn Capital typically invests between £2 million - £5 million in companies that are expanding internationally and are looking for "
    		+ "operational support and capital. Dawn Capital’s portfolio companies include Collibra, Gelato Group, iControl, Mimecast, Neo "
    		+ "Technology, Showpad,  iZettle and Property Partner, with many others looking to join their ranks.\r\n"
    		+ "Headquartered in Copenhagen, Sunstone Capital is an early-stage Life Science and Technology venture capital company investing "
    		+ "in European start-up companies with strong potential to achieve global success in their markets. Since our establishment in 2007, "
    		+ "we have built a strong portfolio currently totaling 50 companies and have completed several successful trade sales and IPOs. With "
    		+ "approximately EUR 700 million in funds under management, Sunstone is today one of the leading and most active independent venture "
    		+ "capital investors in the European and Nordic market.\r\n"
    		+ "Draper Esprit, (formerly DFJ Esprit) member of the The DFJ Network of Partner Funds, is a unique global venture capital model "
    		+ "providing global reach through local presence. It encompasses over 140 venture capital professionals in more than 30 cities "
    		+ "throughout the US, Asia (including China and India), Europe, Israel, South America, and Russia, with more than 600 portfolio "
    		+ "companies funded and over $7 billion of capital under management. Draper Esprit is a single team of partners all of whom benefit "
    		+ "from and contribute to the success of our investments. We are entrepreneurs ourselves and we are proud to have built our firm to "
    		+ "its current size and are passionate about continually moving forward, which means backing winning companies and doing everything we "
    		+ "can to help them achieve and exceed their ambitions.\r\n"
    		+ "Europe is filled with technology and talent. Global innovation in the last century has been driven by European minds: Tesla, Siemens, "
    		+ "Einstein, Planck. Today, Europe still consistently produces world-class talent, but there is a gap between raw ability and the "
    		+ "resources required to build competitive technology and companies. SpeedInvest not only invests capital in world-class talent, but "
    		+ "also provides the entrepreneurial know-how, and operational experience that is rare outside of a place like Silicon Valley, but is "
    		+ "so essential to building great companies.')";
	
	@Test
    public void testEvaluateNull() throws Exception {
        assertEvaluate("sha1(name)", null, Literal.newLiteral(DataTypes.STRING, null));
    }
	
	@Test
    public void testSHA1() throws Exception {
        assertNormalize("sha1('abc')", isLiteral("a9993e364706816aba3e25717850c26c9cd0d89d"));
        assertNormalize("sha1('crate is the best')", isLiteral("65c539a23aa541a7e217b78d9b94a9ac0f58985d"));
        assertNormalize("sha1('last test with umlaut ä ö ü Ö=')", isLiteral("8f870b51c736421aa92dc3febd2df61a345b80f1"));
        assertNormalize( testLongText, isLiteral("d85a11b39e340b6b571f0ccd2df21e13ed059663"));
    }

//Not part of the functionality test routine - only used for performance tests while implementation
//	@Test
//    public void testSHA1Performance() throws Exception {
//		long ms = System.currentTimeMillis();
//		for(int i = 0; i < 50000; i++ ) {
//	        assertNormalize("sha1('abc')", isLiteral("a9993e364706816aba3e25717850c26c9cd0d89d"));
//	        assertNormalize("sha1('crate is the best')", isLiteral("65c539a23aa541a7e217b78d9b94a9ac0f58985d"));
//	        assertNormalize("sha1('last test with umlaut ä ö ü Ö=')", isLiteral("8f870b51c736421aa92dc3febd2df61a345b80f1"));
////	        assertNormalize( testLongText, isLiteral("d85a11b39e340b6b571f0ccd2df21e13ed059663"));
//		}
//		System.out.println( "Time elapsed (in ms): " + (System.currentTimeMillis() - ms) );
//		//Results on local machine (averages with removing the two lowest and highest outliers)
//		//With long text: 
//		// - with MessageDigest: 			 15471ms	(15015/15258/ 15324/15410/15469/15477/15504/15640 /15644/15779)
//		// - with DigestUtils:	 			 15750ms	(15424/15545/ 15673/15724/15741/15757/15777/15827 /15906/15960)
//		// - with DigestUtils + InputStream: 15821ms	(15408/15482/ 15643/15717/15851/15885/15903/15924 /16011/16027)
//		//Without long text:
//		// - with MessageDigest: 			 6655ms		(6401/6603/ 6606/6612/6645/6672/6679/6715 /6741/6748)
//		// - with DigestUtils:	 			 6632ms		(6439/6452/ 6506/6523/6636/6657/6731/6741 /6848/6865)
//		// - with DigestUtils + InputStream: 6641ms		(6382/6458/ 6552/6604/6631/6658/6659/6741 /6774/6829)
//    }
}
