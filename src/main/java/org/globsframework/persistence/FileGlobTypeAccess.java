package org.globsframework.persistence;

import org.globsframework.metamodel.GlobType;

public interface FileGlobTypeAccess {

    GlobType find(String name);

    void declare(GlobType globType);

    void shutdown();
}
