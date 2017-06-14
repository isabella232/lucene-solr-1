package org.apache.solr.servlet;

/*
 *  CLOUDERA ONLY
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class JMXJsonServletTest {

    @Test
    public void testWriteObject() throws IOException {
        JMXJsonServlet sut = new JMXJsonServlet();
        JsonFactory jsonFactory = new JsonFactory();

        List<String[]> cases = Arrays.asList(new String[][]{
                {null, "{\"field\":null}"},
                {"", "{\"field\":\"\"}"},
                {"Password=something", "{\"field\":\"Password=--REDACTED--\"}"},
                {"Password=", "{\"field\":\"Password=--REDACTED--\"}"},
                {"=Something", "{\"field\":\"=Something\"}"},
                {"-Dpassword=something", "{\"field\":\"-Dpassword=--REDACTED--\"}"},
                {"-Dpassword=", "{\"field\":\"-Dpassword=--REDACTED--\"}"},
                {"-D=something", "{\"field\":\"-D=something\"}"},
                {"=", "{\"field\":\"=\"}"}
        });

        for (String[] c : cases) {
            StringWriter out = new StringWriter();
            try (JsonGenerator jg = jsonFactory.createJsonGenerator(out)) {
                jg.writeStartObject();
                sut.writeAttribute(jg, "field", c[0]);

            }
            Assert.assertThat(out.getBuffer().toString(), is(c[1]));
        }

    }

}
