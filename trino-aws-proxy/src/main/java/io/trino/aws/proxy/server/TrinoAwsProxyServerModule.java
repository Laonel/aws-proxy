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

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.server.HttpServerBinder;
import io.airlift.jaxrs.JaxrsBinder;
import io.airlift.log.Logger;
import io.trino.aws.proxy.server.credentials.CredentialsController;
import io.trino.aws.proxy.server.credentials.JsonIdentityProvider;
import io.trino.aws.proxy.server.credentials.file.FileBasedCredentialsModule;
import io.trino.aws.proxy.server.credentials.http.HttpCredentialsModule;
import io.trino.aws.proxy.server.remote.DefaultRemoteS3Module;
import io.trino.aws.proxy.server.rest.LimitStreamController;
import io.trino.aws.proxy.server.rest.ParamProvider;
import io.trino.aws.proxy.server.rest.RequestLoggerController;
import io.trino.aws.proxy.server.rest.ResourceSecurityDynamicFeature;
import io.trino.aws.proxy.server.rest.S3PresignController;
import io.trino.aws.proxy.server.rest.ThrowableMapper;
import io.trino.aws.proxy.server.rest.TrinoLogsResource;
import io.trino.aws.proxy.server.rest.TrinoS3ProxyClient;
import io.trino.aws.proxy.server.rest.TrinoS3ProxyClient.ForProxyClient;
import io.trino.aws.proxy.server.rest.TrinoS3Resource;
import io.trino.aws.proxy.server.rest.TrinoStatusResource;
import io.trino.aws.proxy.server.rest.TrinoStsResource;
import io.trino.aws.proxy.server.security.S3SecurityController;
import io.trino.aws.proxy.server.security.opa.OpaS3SecurityModule;
import io.trino.aws.proxy.server.signing.SigningControllerConfig;
import io.trino.aws.proxy.server.signing.SigningModule;
import io.trino.aws.proxy.spi.credentials.AssumedRoleProvider;
import io.trino.aws.proxy.spi.credentials.CredentialsProvider;
import io.trino.aws.proxy.spi.credentials.Identity;
import io.trino.aws.proxy.spi.credentials.StandardIdentity;
import io.trino.aws.proxy.spi.plugin.TrinoAwsProxyServerPlugin;
import io.trino.aws.proxy.spi.plugin.config.AssumedRoleProviderConfig;
import io.trino.aws.proxy.spi.plugin.config.CredentialsProviderConfig;
import io.trino.aws.proxy.spi.plugin.config.RemoteS3Config;
import io.trino.aws.proxy.spi.plugin.config.S3RequestRewriterConfig;
import io.trino.aws.proxy.spi.plugin.config.S3SecurityFacadeProviderConfig;
import io.trino.aws.proxy.spi.remote.RemoteS3Facade;
import io.trino.aws.proxy.spi.rest.S3RequestRewriter;
import io.trino.aws.proxy.spi.security.S3SecurityFacadeProvider;
import org.glassfish.jersey.server.model.Resource;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.http.server.HttpServerBinder.httpServerBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class TrinoAwsProxyServerModule
        extends AbstractConfigurationAwareModule
{
    private static final Logger log = Logger.get(TrinoAwsProxyServerModule.class);

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(SigningControllerConfig.class);
        TrinoAwsProxyConfig builtConfig = buildConfigObject(TrinoAwsProxyConfig.class);

        JaxrsBinder jaxrsBinder = jaxrsBinder(binder);

        jaxrsBinder.bind(ThrowableMapper.class);
        jaxrsBinder.bind(ParamProvider.class);
        jaxrsBinder.bind(ResourceSecurityDynamicFeature.class);
        bindResourceAtPath(jaxrsBinder, TrinoS3Resource.class, builtConfig.getS3Path());
        bindResourceAtPath(jaxrsBinder, TrinoStsResource.class, builtConfig.getStsPath());
        bindResourceAtPath(jaxrsBinder, TrinoLogsResource.class, builtConfig.getLogsPath());
        bindResourceAtPath(jaxrsBinder, TrinoStatusResource.class, builtConfig.getStatusPath());

        binder.bind(CredentialsController.class).in(Scopes.SINGLETON);
        binder.bind(RequestLoggerController.class).in(Scopes.SINGLETON);
        binder.bind(LimitStreamController.class).in(Scopes.SINGLETON);

        // TODO config, etc.
        httpClientBinder(binder).bindHttpClient("ProxyClient", ForProxyClient.class);
        binder.bind(TrinoS3ProxyClient.class).in(Scopes.SINGLETON);

        HttpServerBinder httpServerBinder = httpServerBinder(binder);
        httpServerBinder.enableLegacyUriCompliance();
        httpServerBinder.enableCaseSensitiveHeaderCache();

        // S3SecurityFacadeProvider binder
        configBinder(binder).bindConfig(S3SecurityFacadeProviderConfig.class);
        newOptionalBinder(binder, S3SecurityFacadeProvider.class).setDefault().toProvider(() -> {
            log.info("Using default %s NOOP implementation", S3SecurityFacadeProvider.class.getSimpleName());
            return S3SecurityFacadeProvider.NOOP;
        });

        // CredentialsProvider binder
        configBinder(binder).bindConfig(CredentialsProviderConfig.class);
        newOptionalBinder(binder, CredentialsProvider.class).setDefault().toProvider(() -> {
            log.info("Using default %s NOOP implementation", CredentialsProvider.class.getSimpleName());
            return CredentialsProvider.NOOP;
        });
        newOptionalBinder(binder, new TypeLiteral<Class<? extends Identity>>() {}).setDefault().toProvider(() -> {
            log.info("Using %s identity type", StandardIdentity.class.getSimpleName());
            return StandardIdentity.class;
        });
        newSetBinder(binder, com.fasterxml.jackson.databind.Module.class).addBinding().toProvider(JsonIdentityProvider.class).in(Scopes.SINGLETON);

        // RequestRewriter binder
        configBinder(binder).bindConfig(S3RequestRewriterConfig.class);
        newOptionalBinder(binder, S3RequestRewriter.class).setDefault().toProvider(() -> {
            log.info("Using default %s NOOP implementation", S3RequestRewriter.class.getSimpleName());
            return S3RequestRewriter.NOOP;
        });

        // provided implementations
        install(new FileBasedCredentialsModule());
        install(new OpaS3SecurityModule());
        install(new HttpCredentialsModule());

        // AssumedRoleProvider binder
        configBinder(binder).bindConfig(AssumedRoleProviderConfig.class);
        // AssumedRoleProvider provided implementations
        newOptionalBinder(binder, AssumedRoleProvider.class).setDefault().toProvider(() -> {
            log.info("Using default %s NOOP implementation", AssumedRoleProvider.class.getSimpleName());
            return AssumedRoleProvider.NOOP;
        });

        // RemoteS3 binder
        newOptionalBinder(binder, RemoteS3Facade.class);
        // RemoteS3 provided implementation
        install(conditionalModule(
                RemoteS3Config.class,
                config -> config.getPluginIdentifier().isEmpty(),
                new DefaultRemoteS3Module()));

        installSigningController(binder);
        installS3SecurityController(binder);

        installPlugins();
        install(new TrinoAwsProxyPluginValidatorModule());

        addNullCollectionModule(binder);
    }

    @Provides
    public XmlMapper newXmlMapper()
    {
        // NOTE: this is _not_ a singleton on purpose. ObjectMappers/XmlMappers are mutable.
        XmlMapper xmlMapper = new XmlMapper();
        xmlMapper.registerModule(new Jdk8Module());
        xmlMapper.setPropertyNamingStrategy(PropertyNamingStrategies.UPPER_CAMEL_CASE);
        return xmlMapper;
    }

    @VisibleForTesting
    protected void installS3SecurityController(Binder binder)
    {
        binder.bind(S3PresignController.class).in(Scopes.SINGLETON);
        binder.bind(S3SecurityController.class).in(Scopes.SINGLETON);
    }

    @VisibleForTesting
    protected void installSigningController(Binder binder)
    {
        install(new SigningModule());
    }

    private void addNullCollectionModule(Binder binder)
    {
        Module module = new SimpleModule()
        {
            @Override
            public void setupModule(SetupContext context)
            {
                context.configOverride(List.class).setSetterInfo(JsonSetter.Value.forValueNulls(Nulls.AS_EMPTY));
                context.configOverride(Set.class).setSetterInfo(JsonSetter.Value.forValueNulls(Nulls.AS_EMPTY));
                context.configOverride(Map.class).setSetterInfo(JsonSetter.Value.forValueNulls(Nulls.AS_EMPTY));
            }
        };
        newSetBinder(binder, Module.class).addBinding().toInstance(module);
    }

    private void installPlugins()
    {
        ServiceLoader.load(TrinoAwsProxyServerPlugin.class)
                .forEach(plugin -> {
                    log.info("Loading plugin: %s", plugin.name());
                    install(plugin.module());
                });
    }

    private static void bindResourceAtPath(JaxrsBinder jaxrsBinder, Class<?> resourceClass, String resourcePrefix)
    {
        jaxrsBinder.bind(resourceClass);
        jaxrsBinder.bindInstance(Resource.builder(resourceClass).path(resourcePrefix).build());
    }
}
