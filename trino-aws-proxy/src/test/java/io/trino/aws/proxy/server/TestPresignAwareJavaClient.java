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

import com.google.common.io.Resources;
import com.google.inject.Inject;
import com.google.inject.Key;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.aws.proxy.server.testing.TestingCredentialsRolesProvider;
import io.trino.aws.proxy.server.testing.TestingIdentity;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules;
import io.trino.aws.proxy.spark4.PresignAwareS3Client;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.Credentials;
import jakarta.ws.rs.core.UriBuilder;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.aws.proxy.server.testing.TestingUtil.clientBuilder;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThatNoException;

/*
Run with environment variables:
AWS_ACCESS_KEY_ID=<accessKey>
AWS_SECRET_ACCESS_KEY=<secretKey>
AWS_REGION=us-east-1

Authorized user must have read / write access to S3 including create bucket
 */
@TrinoAwsProxyTest(filters = {TestPresignAwareJavaClient.Filter.class, TrinoAwsProxyTestCommonModules.WithAwsSFacade.class})
public class TestPresignAwareJavaClient
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
                    .addModule(binder ->
                            newOptionalBinder(binder, Key.get(Credentials.class, ForTesting.class))
                                    .setBinding()
                                    .toInstance(Credentials.build(
                                            new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
                                            new Credential(accessKeyId, secretAccessKey),
                                            new TestingIdentity(UUID.randomUUID().toString(), List.of(), UUID.randomUUID().toString()))));
        }
    }

    private final S3Client s3Client;
    private final Supplier<S3Client> localS3ClientSupplier;

    @Inject
    public TestPresignAwareJavaClient(
            S3Client s3Client,
            @ForTesting Credentials awsCredentials,
            TestingHttpServer httpServer,
            TrinoAwsProxyConfig config,
            TestingCredentialsRolesProvider credentialsController)
    {
        this.s3Client = s3Client;

        localS3ClientSupplier = () -> clientBuilder(UriBuilder.fromUri(httpServer.getBaseUrl()).path(config.getS3Path()).build())
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        awsCredentials.emulated().accessKey(),
                        awsCredentials.emulated().secretKey())))
                .forcePathStyle(true)
                .build();
        credentialsController.addCredentials(awsCredentials);
    }

    @Test
    public void testPresignAwareJavaClient()
            throws Exception
    {
        String bucketName = "test-" + randomUUID();
        // create the test bucket
        s3Client.createBucket(r -> r.bucket(bucketName));
        // upload a CSV file as a potential table
        s3Client.putObject(r -> r.bucket(bucketName).key("table/file.csv"), Path.of(Resources.getResource("test.csv").toURI()));

        try (var presignAwareClient = new PresignAwareS3Client(localS3ClientSupplier.get())) {
            assertThatNoException().isThrownBy(() -> presignAwareClient.getObject(r -> r.bucket(bucketName).key("table/file.csv")));
        }
        finally {
            // delete the test bucket
            s3Client.deleteObject(r -> r.bucket(bucketName).key("table/file.csv"));
            s3Client.deleteBucket(r -> r.bucket(bucketName));
        }
    }
}
