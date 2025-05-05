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
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer;
import io.trino.aws.proxy.server.testing.containers.PySparkContainer;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import java.nio.file.Path;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.aws.proxy.server.testing.containers.DockerAttachUtil.clearInputStreamAndClose;
import static io.trino.aws.proxy.server.testing.containers.DockerAttachUtil.inputToContainerStdin;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

@TrinoAwsProxyTest(filters = TestPySparkSql.Filter.class)
public class TestPySparkSql
{
    public static final String DATABASE_NAME = "db";
    public static final String TABLE_NAME = "people";

    private final S3Client s3Client;
    private final PySparkContainer pySparkContainer;

    public static class Filter
            implements BuilderFilter
    {
        @Override
        public TestingTrinoAwsProxyServer.Builder filter(TestingTrinoAwsProxyServer.Builder builder)
        {
            return builder.withV3PySparkContainer();
        }
    }

    @Inject
    public TestPySparkSql(S3Client s3Client, PySparkContainer pySparkContainer)
    {
        this.s3Client = requireNonNull(s3Client, "s3Client is null");
        this.pySparkContainer = requireNonNull(pySparkContainer, "pySparkContainer is null");
    }

    @Test
    public void testSql()
            throws Exception
    {
        String bucketName = "test-" + randomUUID();
        createDatabaseAndTable(s3Client, pySparkContainer, bucketName);

        try {
            clearInputStreamAndClose(inputToContainerStdin(pySparkContainer.containerId(),
                    "spark.sql(\"select * from %s.%s\").show()".formatted(DATABASE_NAME, TABLE_NAME)), line -> line.equals("|    John Galt| 28|"));

            clearInputStreamAndClose(inputToContainerStdin(pySparkContainer.containerId(), """
                    columns = ['name', 'age']
                    vals = [('a', 10), ('b', 20), ('c', 30)]
                    
                    df = spark.createDataFrame(vals, columns)
                    df.write.insertInto('%s.%s')
                    """.formatted(DATABASE_NAME, TABLE_NAME)), line -> line.contains("Stage 1:"));

            clearInputStreamAndClose(inputToContainerStdin(pySparkContainer.containerId(),
                    "spark.sql(\"select * from %s.%s\").show()".formatted(DATABASE_NAME, TABLE_NAME)), line -> line.equals("|            c| 30|"));
        }
        finally {
            s3Client.listObjectsV2Paginator(r -> r.bucket(bucketName)).stream()
                    .map(ListObjectsV2Response::contents)
                    .forEach(s3Objects -> s3Client.deleteObjects(r -> r
                            .bucket(bucketName)
                            .delete(Delete.builder().objects(s3Objects.stream()
                                    .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
                                    .collect(toImmutableSet())).build())));
            s3Client.deleteBucket(r -> r.bucket(bucketName));
        }
    }

    public static void createDatabaseAndTable(S3Client s3Client, PySparkContainer container, String bucketName)
            throws Exception
    {
        // create the test bucket
        s3Client.createBucket(r -> r.bucket(bucketName));

        // upload a CSV file as a potential table
        s3Client.putObject(r -> r.bucket(bucketName).key("table/file.csv"), Path.of(Resources.getResource("test.csv").toURI()));

        // create the database
        clearInputStreamAndClose(inputToContainerStdin(container.containerId(), "spark.sql(\"create database %s\")".formatted(DATABASE_NAME)), line -> line.equals("DataFrame[]"));

        // create the DB
        clearInputStreamAndClose(inputToContainerStdin(container.containerId(), """
                spark.sql(""\"
                  CREATE TABLE IF NOT EXISTS %s.%s(name STRING, age INT)
                  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                  LOCATION 's3a://%s/table/'
                  TBLPROPERTIES ("s3select.format" = "csv");
                  ""\")
                """.formatted(DATABASE_NAME, TABLE_NAME, bucketName)), line -> line.equals("DataFrame[]"));
    }
}
