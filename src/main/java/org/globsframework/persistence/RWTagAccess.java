package org.globsframework.persistence;

import org.globsframework.model.Glob;

import java.util.Iterator;

public interface RWTagAccess {
    void save(String uuid, Glob[] tags);

    void listAll(TagListener consumer);

    void shutdown();

    interface TagListener {
        void accept(String uuid, Iterator<Glob> tags);
    }
}
