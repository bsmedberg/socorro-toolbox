/*
 * Copyright 2012 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mozilla.socorro.pig.eval;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class LookupFirstSourceFrameTest {

    private LookupFirstSourceFrame lfsf = new LookupFirstSourceFrame();
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    private String readFile(String file) throws IOException {
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        try {
            long len = new File(file).length();
            byte[] bytes = new byte[(int) len];
            dis.readFully(bytes);
            return new String(bytes, "UTF-8");
        }
        finally {
            dis.close();
        }
    }

    @Test
    public void testExec1() throws IOException {
        Tuple input = tupleFactory.newTuple();
        input.append(readFile(System.getProperty("basedir") + "/src/test/resources/lfsft1.dump"));
        input.append(4);

        String r = lfsf.exec(input);

        assertEquals("nsExpirationTracker<gfxTextRun, int>::RemoveObject", r);
    }

    @Test
    public void testExec2() throws IOException {
        Tuple input = tupleFactory.newTuple();
        input.append(readFile(System.getProperty("basedir") + "/src/test/resources/lfsft2.dump"));
        input.append(0);

        String r = lfsf.exec(input);

        assertEquals("NS_GetWeakReference(nsISupports*, unsigned int*)", r);
    }
}
