/*
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

package org.apache.solr.servlet;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.ServletInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

public class TeeHttpServletRequestWrapperTest extends SolrTestCaseJ4 {

  @Test
  public void testTeeInputStream() throws Exception {
    HttpServletRequest request = createMock(HttpServletRequest.class);
    byte [] foobar = "foobar".getBytes("UTF-8");
    final InputStream inputStream = new ByteArrayInputStream(foobar);
    final ServletInputStream sis = new ServletInputStream() {
      public int read() throws IOException {
        return inputStream.read();
      }
    };
    expect(request.getInputStream()).andReturn(sis).anyTimes();
    replay(request);
    TeeHttpServletRequestWrapper tee = new TeeHttpServletRequestWrapper(request);

    // read part of the first inputStream
    int firstReadLength = foobar.length / 2;
    InputStream firstInputStream = tee.getInputStream();
    for (int i = 0; i < firstReadLength; ++i) {
      firstInputStream.read();
    }

    // Ensure the second called to getInputStream contains the full stream
    assertTrue(Arrays.equals(foobar, IOUtils.toByteArray(tee.getInputStream())));
  }
}
