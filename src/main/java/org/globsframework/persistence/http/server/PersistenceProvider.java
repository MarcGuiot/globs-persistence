package org.globsframework.persistence.http.server;

import com.google.gson.Gson;
import org.globsframework.persistence.FileGlobTypeAccess;
import org.globsframework.persistence.Persistence;

public interface PersistenceProvider {
    PersistenceInfo get(String typeName);

    static class PersistenceInfo {
        public final Persistence persistence;
        public final FileGlobTypeAccess fileGlobTypeAccess;
        public final Gson gson;

        public PersistenceInfo(Persistence persistence, FileGlobTypeAccess fileGlobTypeAccess, Gson gson) {
            this.persistence = persistence;
            this.fileGlobTypeAccess = fileGlobTypeAccess;
            this.gson = gson;
        }
    }
}
