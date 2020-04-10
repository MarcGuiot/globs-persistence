package org.globsframework.persistence.http.client;

import org.globsframework.json.GlobTypeResolver;
import org.globsframework.metamodel.GlobType;
import org.globsframework.persistence.Persistence;

public interface PersistenceRemoteClient {

    void createClient(GlobType type, GlobTypeResolver typeResolver);

    ClientPersistence get(GlobType type);

    void shutdown();

}
