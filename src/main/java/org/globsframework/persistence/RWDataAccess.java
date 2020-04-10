package org.globsframework.persistence;

import org.globsframework.model.Glob;

public interface RWDataAccess {
    Glob getData(String uuid);

    String save(Glob glob); //return UUID

    void shutdown();
}
