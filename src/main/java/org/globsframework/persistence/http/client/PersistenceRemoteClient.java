package org.globsframework.persistence.http.client;

import org.globsframework.metamodel.GlobTypeResolver;
import org.globsframework.metamodel.GlobType;

public interface PersistenceRemoteClient {

    void createClient(GlobType type, GlobTypeResolver typeResolver);

    ClientPersistence get(GlobType type);

    void shutdown();

}
