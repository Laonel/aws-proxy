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
package io.trino.aws.proxy.spi.plugin.config;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public class RemoteS3ConnectionProviderConfig
        implements PluginIdentifierConfig
{
    private Optional<String> identifier = Optional.empty();

    @NotNull
    @Override
    public Optional<String> getPluginIdentifier()
    {
        return identifier;
    }

    @Config("remote-s3-connection-provider.type")
    public void setPluginIdentifier(String identifier)
    {
        this.identifier = Optional.ofNullable(identifier);
    }
}
