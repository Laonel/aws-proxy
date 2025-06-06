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
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.aws.proxy.server.testing.TestingCredentialsRolesProvider;
import io.trino.aws.proxy.server.testing.TestingS3RequestRewriteController;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithConfiguredBuckets;
import io.trino.aws.proxy.spi.credentials.Credential;
import org.junit.jupiter.api.AfterAll;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@TrinoAwsProxyTest(filters = WithConfiguredBuckets.class)
public class TestProxiedAssumedRoleRequests
        extends AbstractTestProxiedRequests
{
    private static final String ARN = "arn:aws:iam::123456789012:role/assumed";
    private final TestingCredentialsRolesProvider credentialsController;

    @Inject
    public TestProxiedAssumedRoleRequests(
            TestingHttpServer httpServer,
            @ForTesting Credential testingCredential,
            TestingCredentialsRolesProvider credentialsController,
            @ForS3Container S3Client storageClient,
            TrinoAwsProxyConfig trinoAwsProxyConfig,
            TestingS3RequestRewriteController requestRewriteController)
    {
        this(buildClient(httpServer, testingCredential, trinoAwsProxyConfig.getS3Path(), trinoAwsProxyConfig.getStsPath()), credentialsController, storageClient,
                requestRewriteController);
    }

    protected TestProxiedAssumedRoleRequests(
            S3Client internalClient,
            TestingCredentialsRolesProvider credentialsController,
            S3Client storageClient,
            TestingS3RequestRewriteController requestRewriteController)
    {
        super(internalClient, storageClient, requestRewriteController);

        this.credentialsController = requireNonNull(credentialsController, "credentialsController is null");
    }

    @AfterAll
    public void validateCount()
    {
        assertThat(credentialsController.assumedRoleCount()).isGreaterThan(0);
        credentialsController.resetAssumedRoles();
    }

    protected static S3Client buildClient(TestingHttpServer httpServer, Credential credential, String s3Path, String stsPath)
    {
        URI baseUrl = httpServer.getBaseUrl();
        URI localProxyServerUri = baseUrl.resolve(s3Path);
        URI localStsServerUri = baseUrl.resolve(stsPath);

        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(credential.accessKey(), credential.secretKey());

        StsClient stsClient = StsClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials))
                .endpointProvider(endpointParams -> CompletableFuture.completedFuture(Endpoint.builder().url(localStsServerUri).build()))
                .build();

        StsAssumeRoleCredentialsProvider credentialsProvider = StsAssumeRoleCredentialsProvider.builder()
                .refreshRequest(request -> request
                        .roleArn(ARN)
                        .roleSessionName("dummy")
                        .externalId("dummy"))
                .stsClient(stsClient)
                .asyncCredentialUpdateEnabled(true)
                .build();

        return S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(credentialsProvider)
                .endpointOverride(localProxyServerUri)
                .build();
    }
}
