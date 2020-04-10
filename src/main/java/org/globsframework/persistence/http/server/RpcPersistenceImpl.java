package org.globsframework.persistence.http.server;

import com.google.gson.Gson;
import org.globsframework.metamodel.GlobType;
import org.globsframework.model.Glob;
import org.globsframework.model.MutableGlob;
import org.globsframework.persistence.FileGlobTypeAccess;
import org.globsframework.persistence.Persistence;
import org.globsframework.persistence.http.DataEvent;
import org.globsframework.persistence.http.RpcPersistence;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.utils.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class RpcPersistenceImpl implements RpcPersistence {
    static final Logger LOGGER = LoggerFactory.getLogger(RpcPersistenceImpl.class);
    private final PersistenceProvider persistenceProvider;
    private Map<String, PersistenceHttpServer.PerClientInfo> clientListeners = new ConcurrentHashMap<>();

    RpcPersistenceImpl(PersistenceProvider persistenceProvider) {
        this.persistenceProvider = persistenceProvider;
    }

    public void declareType(String type, String typesAsJson) {
        LOGGER.info("declareType " + type + "  " + typesAsJson);
        PersistenceProvider.PersistenceInfo persistenceInfo = persistenceProvider.get(type);
        FileGlobTypeAccess typeAccess = persistenceInfo.fileGlobTypeAccess;
        Arrays.stream(persistenceInfo.gson.fromJson(typesAsJson, GlobType[].class))
                .forEach(typeAccess::declare);
    }

    public String pushData(String type, String jsonData, String jsonTags) {
        LOGGER.info("pushData " + type + "  " + jsonData + " " + jsonTags);
        PersistenceProvider.PersistenceInfo persistenceInfo = persistenceProvider.get(type);
        Glob data = persistenceInfo.gson.fromJson(jsonData, Glob.class);
        MutableGlob[] tags = persistenceInfo.gson.fromJson(jsonTags, MutableGlob[].class);
        return persistenceInfo.persistence.pushData(data, tags);
    }

    public String list(String typeName, String name, String constraintAsJson) {
        LOGGER.info("list " + typeName + "  " + name + " " + constraintAsJson);
        PersistenceProvider.PersistenceInfo persistenceInfo = persistenceProvider.get(typeName);
        List<Glob> data = persistenceInfo.persistence.list(persistenceInfo.fileGlobTypeAccess.find(name),
                persistenceInfo.gson.fromJson(constraintAsJson, Constraint.class));
        return persistenceInfo.gson.toJson(data);
    }

    public String register(String listenerUUID, String typeName, String listenerTypeName, String constraintAsJson, String[] additionalTypes) {
        LOGGER.info("register " + typeName + "  " + listenerTypeName + " " + constraintAsJson);
        PersistenceProvider.PersistenceInfo persistenceInfo = persistenceProvider.get(typeName);
        PersistenceHttpServer.PerClientInfo perClientInfo = clientListeners.computeIfAbsent(listenerUUID, s -> new PersistenceHttpServer.PerClientInfo(s, persistenceInfo));
        String uuid = UUID.randomUUID().toString();
        Persistence.Listener listen = persistenceInfo.persistence.listen(persistenceInfo.fileGlobTypeAccess.find(listenerTypeName),
                persistenceInfo.gson.fromJson(constraintAsJson, Constraint.class),
                new Persistence.OnChange() {
                    public void change(GlobType type, Glob oldValue, Glob newValue, List<Glob> additionalTags) {
                        perClientInfo.onChange(uuid, type, oldValue, newValue, additionalTags);
                    }
                }, Arrays.stream(additionalTypes).map(persistenceInfo.fileGlobTypeAccess::find).toArray(GlobType[]::new));
        perClientInfo.register(uuid, listen);
        return uuid;
    }

    public void unregister(String listenerUUID, String uuid) {
        LOGGER.info("unregister " + listenerUUID + "  " + uuid);
        PersistenceHttpServer.PerClientInfo perClientInfo = clientListeners.get(listenerUUID);
        if (perClientInfo != null) {
            perClientInfo.unregister(uuid);
        }
    }

    public String getData(String typeName, String tag) {
        LOGGER.info("getData " + typeName + "  " + tag);
        PersistenceProvider.PersistenceInfo persistenceInfo = persistenceProvider.get(typeName);
        Gson gson = persistenceInfo.gson;
        return gson.toJson(persistenceInfo.persistence.getData(gson.fromJson(tag, Glob.class)));
    }

    public String updateTag(String typeName, String refTag, String tags) {
        LOGGER.info("updateTag " + typeName + "  " + refTag + " " + tags);
        PersistenceProvider.PersistenceInfo persistenceInfo = persistenceProvider.get(typeName);
        Gson gson = persistenceInfo.gson;
        return persistenceInfo.persistence.updateTag(gson.fromJson(refTag, Glob.class),
                gson.fromJson(tags, MutableGlob[].class));
    }

    public long getNextEvents(String listenerUUID, long lastId, Ref<List<DataEvent>> events) {
        LOGGER.info("getNextEvents " + listenerUUID);
        PersistenceHttpServer.PerClientInfo perClientInfo = clientListeners.get(listenerUUID);
        if (perClientInfo != null) {
            events.set(perClientInfo.drain());
        }
        return 0;
    }
}
