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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.sessioncatalog.SessionCatalogMetadata;

import java.util.List;
import java.util.Map;

public class NoOpSessionCatalog
        implements SessionCatalog
{
    @Override
    public Map<String, Catalog> getAndLoadSessionCatalog(Session session)
    {
        return ImmutableMap.of();
    }

    @Override
    public void loadSessionCatalog(List<SessionCatalogMetadata> sessionCatalogMetadataList)
    {
    }
}
