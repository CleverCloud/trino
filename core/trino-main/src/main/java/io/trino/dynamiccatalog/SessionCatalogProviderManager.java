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
package io.trino.dynamiccatalog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.spi.security.Identity;
import io.trino.spi.sessioncatalog.CatalogProviderFactory;
import io.trino.spi.sessioncatalog.SessionCatalogMetadata;
import io.trino.spi.sessioncatalog.SessionCatalogProvider;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static java.util.Objects.requireNonNull;

public class SessionCatalogProviderManager
        implements SessionCatalogProvider
{
    private static final Logger log = Logger.get(SessionCatalogProviderManager.class);
    private static final File CATALOG_PROVIDER_CONFIGURATION = new File("etc/catalog-provider.properties");
    private static final String CATALOG_PROVIDER_PROPERTY_NAME = "catalog-provider.name";
    private final Map<String, CatalogProviderFactory> dynamicCatalogFactories = new ConcurrentHashMap<>();
    private final AtomicReference<Optional<SessionCatalogProvider>> configuredSessionCatalogProvider = new AtomicReference<>(Optional.empty());

    public void addDynamicCatalogFactory(CatalogProviderFactory catalogProviderFactory)
    {
        requireNonNull(catalogProviderFactory, "dynamicCatalogFactory is null");

        if (dynamicCatalogFactories.putIfAbsent(catalogProviderFactory.getName(), catalogProviderFactory) != null) {
            throw new IllegalArgumentException(String.format("Dynamic catalog '%s' is already registered", catalogProviderFactory.getName()));
        }
    }

    public void loadConfiguredCatalogProvider()
            throws IOException
    {
        loadConfiguredCatalogProvider(CATALOG_PROVIDER_CONFIGURATION);
    }

    @VisibleForTesting
    private void loadConfiguredCatalogProvider(File catalogProviderConfiguration) throws IOException
    {
        if (configuredSessionCatalogProvider.get().isPresent() || !catalogProviderConfiguration.exists()) {
            return;
        }

        Map<String, String> properties = new HashMap<>(loadPropertiesFrom(catalogProviderConfiguration.getPath()));

        String catalogProviderName = properties.remove(CATALOG_PROVIDER_PROPERTY_NAME);
        checkArgument(!isNullOrEmpty(catalogProviderName),
                "Catalog provider configuration %s does not contain %s", catalogProviderConfiguration.getAbsoluteFile(), CATALOG_PROVIDER_PROPERTY_NAME);

        setConfiguredCatalogProvider(catalogProviderName, properties);
    }

    private void setConfiguredCatalogProvider(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        log.info("-- Loading catalog provider %s --", name);

        CatalogProviderFactory catalogProviderFactory = dynamicCatalogFactories.get(name);
        checkState(catalogProviderFactory != null, "Catalog provider %s is not registered", name);

        SessionCatalogProvider sessionCatalogProvider = catalogProviderFactory.create(ImmutableMap.copyOf(properties));

        checkState(configuredSessionCatalogProvider.compareAndSet(Optional.empty(), Optional.of(sessionCatalogProvider)), "Provider is already set");

        log.info("-- Loaded group provider %s --", name);
    }

    @Override
    public List<SessionCatalogMetadata> getAvailableCatalogs(Identity identity, Optional<String> traceToken, String queryId)
    {
        return configuredSessionCatalogProvider.get()
                .map(provider -> provider.getAvailableCatalogs(identity, traceToken, queryId))
                .map(ImmutableList::copyOf)
                .orElse(ImmutableList.of());
    }
}
