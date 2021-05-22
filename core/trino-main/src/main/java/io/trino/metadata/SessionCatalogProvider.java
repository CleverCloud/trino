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
package io.trino.metadata;

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.connector.ConnectorManager;
import io.trino.dynamiccatalog.SessionCatalogProviderManager;
import io.trino.spi.security.Identity;
import io.trino.spi.sessioncatalog.SessionCatalogMetadata;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class SessionCatalogProvider
        implements SessionCatalog
{
    private static final Logger LOG = Logger.get(SessionCatalogProvider.class);
    private final SessionCatalogProviderManager sessionCatalogProviderManager;
    private final ConnectorManager connectorManager;

    @Inject
    public SessionCatalogProvider(SessionCatalogProviderManager sessionCatalogProviderManager, ConnectorManager connectorManager)
    {
        this.sessionCatalogProviderManager = sessionCatalogProviderManager;
        this.connectorManager = connectorManager;
    }

    public Map<String, Catalog> getAndLoadSessionCatalog(Session session)
    {
        Identity identity = session.getIdentity();
        Optional<String> traceToken = session.getTraceToken();
        LOG.info("searching for session catalog with identity %s", identity.getUser());
        return sessionCatalogProviderManager.getAvailableCatalogs(identity, traceToken, session.getQueryId().getId()).stream()
                .peek(session::addSessionCatalogProperties)
                .map(connectorManager::createOptionalSessionCatalog)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .peek(catalog -> LOG.info("found sessionCatalog %s for identity %s", catalog.getCatalogName(), identity.getUser()))
                .collect(Collectors.toMap(Catalog::getCatalogName, e -> e));
    }

    @Override
    public void loadSessionCatalog(List<SessionCatalogMetadata> sessionCatalogMetadataList)
    {
        sessionCatalogMetadataList.forEach(connectorManager::createOptionalSessionCatalog);
    }
}
