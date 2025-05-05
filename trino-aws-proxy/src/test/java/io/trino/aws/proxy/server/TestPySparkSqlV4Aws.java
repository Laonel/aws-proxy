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
package io.trino.aws.proxy.server;

import com.google.inject.Inject;
import com.google.inject.Key;
import io.trino.aws.proxy.server.testing.TestingCredentialsRolesProvider;
import io.trino.aws.proxy.server.testing.TestingIdentity;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.server.testing.containers.PySparkContainer;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithAwsSFacade;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.Credentials;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;
import java.util.UUID;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.util.Objects.requireNonNull;

/*
Run with environment variables:
AWS_ACCESS_KEY_ID=<accessKey>
AWS_SECRET_ACCESS_KEY=<secretKey>
AWS_REGION=us-east-1

Authorized user must have read / write access to S3 including create bucket
 */
@TrinoAwsProxyTest(filters = {TestPySparkSqlV4Aws.Filter.class, WithAwsSFacade.class})
public class TestPySparkSqlV4Aws
        extends TestPySparkSql
{
    public static class Filter
            implements BuilderFilter
    {
        @Override
        public TestingTrinoAwsProxyServer.Builder filter(TestingTrinoAwsProxyServer.Builder builder)
        {
            String accessKeyId = requireNonNull(System.getenv("AWS_ACCESS_KEY_ID"), "AWS_ACCESS_KEY_ID is not set");
            String secretAccessKey = requireNonNull(System.getenv("AWS_SECRET_ACCESS_KEY"), "AWS_SECRET_ACCESS_KEY is not set");

            return builder
                    .withV4PySparkContainer()
                    .addModule(binder ->
                            newOptionalBinder(binder, Key.get(Credentials.class, ForTesting.class))
                                    .setBinding()
                                    .toInstance(Credentials.build(
                                            new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
                                            new Credential(accessKeyId, secretAccessKey),
                                            new TestingIdentity(UUID.randomUUID().toString(), List.of(), UUID.randomUUID().toString()))));
        }
    }

    @Inject
    public TestPySparkSqlV4Aws(
            S3Client s3Client,
            PySparkContainer pySparkContainer,
            TestingCredentialsRolesProvider credentialsController,
            @ForTesting Credentials awsCredentials)
    {
        super(s3Client, pySparkContainer);
        credentialsController.addCredentials(awsCredentials);
    }
}
