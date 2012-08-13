/**
 * Copyright 2010 Mozilla Foundation
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaUtil;

public class LookupFirstSourceFrame extends EvalFunc<String> {

    private static final Pattern newlinePattern = Pattern.compile("\n");
    private static final Pattern pipePattern = Pattern.compile("\\|");
    
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() != 2) {
            return null;
        }

        if (!(input.get(0) instanceof String)) {
            return null;
        }
        if (!(input.get(1) instanceof Integer)) {
            return null;
        }
        String dump = (String) input.get(0);
        int threadId = (Integer) input.get(1);

        for (String dumpline : newlinePattern.split(dump)) {
            String[] splits = pipePattern.split(dumpline, -1);
            if (splits.length == 0 || splits[0].length() == 0 ||
                !Character.isDigit(splits[0].charAt(0))) {
                continue;
            }

            // threadno, frameno, module, function, srcfile, line, offset
            if (splits.length != 7) {
                continue;
            }

            int id = Integer.parseInt(splits[0]);
            if (id != threadId) {
                continue;
            }

            if (splits[4].length() != 0) {
                return splits[3];
            }
        }

        return null;
    }
}
