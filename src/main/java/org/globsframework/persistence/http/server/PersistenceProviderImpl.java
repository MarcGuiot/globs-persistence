package org.globsframework.persistence.http.server;

import com.google.gson.GsonBuilder;
import org.globsframework.json.GlobsGson;
import org.globsframework.metamodel.GlobModel;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.MutableGlobModel;
import org.globsframework.metamodel.annotations.AllAnnotations;
import org.globsframework.metamodel.impl.DefaultGlobModel;
import org.globsframework.persistence.InMemoryPersistence;
import org.globsframework.persistence.file.DefaultFileGlobTypeAccess;
import org.globsframework.persistence.file.FileRWDataAccess;
import org.globsframework.persistence.file.FileRWTagAccess;
import org.globsframework.sqlstreams.json.JSonConstraintTypeAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class PersistenceProviderImpl implements PersistenceProvider {
    private static Logger LOGGER = LoggerFactory.getLogger(PersistenceProviderImpl.class);
    private Map<String, PersistenceInfo> persistence = new ConcurrentHashMap<>();
    private Path rootDir;
    private static MutableGlobModel annotations;

    static {
        annotations = new DefaultGlobModel();
        AllAnnotations.MODEL.forEach(annotations::add);
        org.globsframework.sqlstreams.annotations.AllAnnotations.MODEL.forEach(annotations::add);
        org.globsframework.json.annottations.AllAnnotations.MODEL.forEach(annotations::add);
    }

    PersistenceProviderImpl(Path rootDir) {
        this.rootDir = rootDir;
        File directory = rootDir.toFile();
        if (!directory.exists()) {
            throw new RuntimeException(directory.getAbsolutePath() + " must exist");
        }
    }

    public PersistenceInfo get(String typeName) {
        return persistence.computeIfAbsent(typeName, new PersistenceBuilder());
    }

    public void shutdown() {
        for (PersistenceInfo persistenceInfo : persistence.values()) {
            persistenceInfo.fileGlobTypeAccess.shutdown();
            persistenceInfo.persistence.shutdown();
        }
    }

    private class PersistenceBuilder implements Function<String, PersistenceInfo> {

        public PersistenceInfo apply(String type) {
            LOGGER.info("Creating Persistence for " + type);
            File directory = new File(rootDir.toFile(), type);
            createDir(directory);
            File schemaDir = new File(directory, "schema");
            createDir(schemaDir);
            File tagsDir = new File(directory, "tags");
            createDir(tagsDir);
            File dataDir = new File(directory, "data");
            createDir(dataDir);
            DefaultFileGlobTypeAccess fileGlobTypeAccess = new DefaultFileGlobTypeAccess(schemaDir.toPath());
            GsonBuilder builder = GlobsGson.createBuilder(name -> {
                GlobType type1 = fileGlobTypeAccess.find(name);
                if (type1 != null) {
                    return type1;
                }
                else {
                    return annotations.findType(name);
                }
            }, true);
            JSonConstraintTypeAdapter.register(builder, fileGlobTypeAccess::find);
            return new PersistenceInfo(new InMemoryPersistence(new FileRWDataAccess(dataDir, fileGlobTypeAccess::find),
                    new FileRWTagAccess(dataDir.toPath(), fileGlobTypeAccess::find)), fileGlobTypeAccess, builder.create());
        }

        private void createDir(File directory) {
            if (!directory.exists()) {
                if (!directory.mkdirs()) {
                    throw new RuntimeException("Fail to crate dir " + directory.getAbsolutePath());
                }
            }
        }
    }
}
