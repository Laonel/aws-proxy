/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.aws.proxy.spi.credentials;

import java.util.Optional;

// TODO: Add back file-based provider
public interface CredentialsProvider
{
    CredentialsProvider NOOP = (_, _) -> Optional.empty();

    /**
     * Return the credentials, if any, for the given access key and session.
     * Your implementation should have a centralized credentials mechanism, likely
     * some type of database along with a way of registering credentials, etc.
     */
    Optional<IdentityCredential> credentials(String emulatedAccessKey, Optional<String> session);
}
