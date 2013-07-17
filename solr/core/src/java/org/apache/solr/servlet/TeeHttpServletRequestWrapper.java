package org.apache.solr.servlet;
/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.TeeInputStream;

/**
 * HttpServletRequestHandler that supports "teeing" a copy of the InputStream
 * returned via getInputStream() so that the second call to getInputStream
 * contains the entire stream.
 */
public class TeeHttpServletRequestWrapper extends HttpServletRequestWrapper {
  private InputStream currentInputStream;
  private ByteArrayOutputStream outputStream;

  // has the TeeInputStream already been returned?  If so, need to return the already read bytes
  private boolean usedTeeInputStream;

  public TeeHttpServletRequestWrapper(HttpServletRequest request) throws IOException {
    super(request);
    InputStream inputStream = super.getInputStream();
    outputStream = new ByteArrayOutputStream();
    currentInputStream = new TeeInputStream(inputStream, outputStream, true);
  }

  @Override
  public ServletInputStream getInputStream() throws IOException {
    if (usedTeeInputStream) {
      // getInputStream has already been called; we need to return an input stream
      // that contains the bytes read from the TeeInputStream
      InputStream prevInput = new ByteArrayInputStream(outputStream.toByteArray());
      SequenceInputStream seqInputStream =
        new SequenceInputStream(prevInput, super.getInputStream());
      currentInputStream.close();
      currentInputStream = seqInputStream;
    }
    usedTeeInputStream = true;
    final InputStream inputStream = currentInputStream;

    ServletInputStream sis = new ServletInputStream() {
      public int read() throws IOException {
        return inputStream.read();
      }
    };
    return sis;
  }
}
