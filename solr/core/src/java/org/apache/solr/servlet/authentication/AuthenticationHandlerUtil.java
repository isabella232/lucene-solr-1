/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.solr.servlet.authentication;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.security.authentication.server.AuthenticationHandler;

/**
 * This is a utility class designed to provide functionality related to
 * {@link AuthenticationHandler}.
 */
public class AuthenticationHandlerUtil {
  /**
   * This method returns authentication type(s) supported by a specified {@link AuthenticationHandler}
   * implementation. This method is specifically designed to work with {@link MultiSchemeAuthenticationHandler}
   * implementation which supports multiple authentication schemes while the {@link AuthenticationHandler}
   * interface supports fetching a single type via {@linkplain AuthenticationHandler#getType()} method.
   *
   * @param handler The authentication handler whose type(s) need to be fetched.
   * @return type(s) associated with the specified authentication handler.
   */
  public static Collection<String> getTypes(AuthenticationHandler handler) {
    if((handler instanceof MultiSchemeAuthenticationHandler)
        || (handler instanceof DelegationTokenMultiSchemeAuthHandler)) {
      return MultiSchemeAuthenticationHandler.getTypes();
    } else {
      return Arrays.asList(handler.getType());
    }
  }
}
