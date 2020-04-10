package org.globsframework.persistence.http.client;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.globsframework.directory.DefaultDirectory;
import org.globsframework.directory.Directory;
import org.globsframework.json.GlobTypeResolver;
import org.globsframework.json.GlobsGson;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.metamodel.type.DataType;
import org.globsframework.model.Glob;
import org.globsframework.model.MutableGlob;
import org.globsframework.persistence.http.DataEvent;
import org.globsframework.persistence.http.RpcPersistence;
import org.globsframework.persistence.http.server.PersistenceHttpServer;
import org.globsframework.remote.DirectoryProvider;
import org.globsframework.remote.peer.direct.DirectPeerToPeer;
import org.globsframework.remote.rpc.RpcService;
import org.globsframework.remote.rpc.impl.DefaultRpcService;
import org.globsframework.remote.shared.AddressAccessor;
import org.globsframework.remote.shared.SharedDataManager;
import org.globsframework.remote.shared.impl.DefaultSharedDataManager;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.json.JSonConstraintTypeAdapter;
import org.globsframework.utils.NanoChrono;
import org.globsframework.utils.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DefaultPersistenceRemoteClient implements PersistenceRemoteClient {
    public static Logger LOGGER = LoggerFactory.getLogger(DefaultPersistenceRemoteClient.class);
    private RpcService rpcService;
    private RpcListener rpcListener;
    private RpcPersistence rpcPersistence;
    private Map<GlobType, ClientPersistence> persistenceMap = new ConcurrentHashMap<>();
    private final ExecutorService executorService;

    public DefaultPersistenceRemoteClient(String host, int port) {
        SharedDataManager sharedDataManager = DefaultSharedDataManager.create(AddressAccessor.FixAddressAccessor.create(host, port));
        Directory directory = new DefaultDirectory();
        DirectPeerToPeer peerToPeer = new DirectPeerToPeer();
        rpcService = new DefaultRpcService("worker", new DirectoryProvider() {
            public Directory getDirectory() {
                return directory;
            }
        }, sharedDataManager, peerToPeer);

        rpcPersistence = rpcService.getService(RpcPersistence.class, PersistenceHttpServer.UNIQUE, new DataEvent.EventsSerializer());
        rpcListener = new RpcListener(rpcPersistence);
        executorService = Executors.newFixedThreadPool(1);
        executorService.submit(rpcListener::run);
    }

    public void createClient(GlobType type, GlobTypeResolver typeResolver) {
        HttpClientPersistence httpClientPersistence = new HttpClientPersistence(type.getName(),
                rpcPersistence, typeResolver, rpcListener);
        persistenceMap.put(type, httpClientPersistence);
    }

    public ClientPersistence get(GlobType type) {
        return persistenceMap.get(type);
    }

    public void shutdown() {
        rpcListener.stop();
        executorService.shutdown();
    }

    private static class HttpClientPersistence implements ClientPersistence {
        final RpcPersistence rpcPersistence;
        private final Gson gson;
        private String typeName;
        private GlobTypeResolver typeResolver;
        private RpcListener rpcListener;
        private Map<String, GlobType> alreadySent = new HashMap<>();

        private HttpClientPersistence(String typeName, RpcPersistence rpcPersistence, GlobTypeResolver typeResolver, RpcListener rpcListener) {
            this.typeName = typeName;
            this.rpcPersistence = rpcPersistence;
            this.typeResolver = typeResolver;
            this.rpcListener = rpcListener;
            GsonBuilder builder = GlobsGson.createBuilder(new GlobTypeResolver() {
                public GlobType find(String name) {
                    GlobType type = typeResolver.find(name);
                    return type != null ? type : alreadySent.get(name);
                }
            }, true);
            JSonConstraintTypeAdapter.register(builder, typeResolver);
            gson = builder.create();
        }

        public void pushData(Glob data, MutableGlob[] tags) {
            LOGGER.info("pushData");
            sendTypes(data, tags);
            String d1 = gson.toJson(data);
            String t2 = gson.toJson(tags);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(d1);
                LOGGER.debug(t2);
            }
            String uuid = rpcPersistence.pushData(typeName, d1, t2);
            LOGGER.info("pushData: " + uuid);
            for (MutableGlob tag : tags) {
                tag.set(getUUIDField(tag), uuid);
            }
        }

        synchronized private void sendTypes(Glob data, Glob[] tags) {
            Set<GlobType> toSend = null;
            if (!alreadySent.containsKey(data.getType().getName())) {
                alreadySent.put(data.getType().getName(), data.getType());
                toSend = new HashSet<>();
                toSend.add(data.getType());
            }
            for (Glob tag : tags) {
                if (!alreadySent.containsKey(tag.getType().getName())) {
                    if (toSend == null) {
                        toSend = new HashSet<>();
                    }
                    alreadySent.put(tag.getType().getName(), tag.getType());
                    toSend.add(tag.getType());
                }
            }
            if (toSend == null) {
                return;
            }
            for (GlobType globType : new HashSet<>(toSend)) {
                getAllSubType(globType, alreadySent, toSend);
            }
            LOGGER.info("publish types on " + typeName);
            rpcPersistence.declareType(typeName, gson.toJson(toSend.toArray(new GlobType[0])));
        }

        synchronized private void sendTypes(GlobType data, GlobType[] tags) {
            Set<GlobType> toSend = null;
            if (!alreadySent.containsKey(data.getName())) {
                toSend = new HashSet<>();
                toSend.add(data);
                alreadySent.put(data.getName(), data);
            }
            for (GlobType tag : tags) {
                if (!alreadySent.containsKey(tag)) {
                    if (toSend == null) {
                        toSend = new HashSet<>();
                    }
                    alreadySent.put(tag.getName(), tag);
                    toSend.add(tag);
                }
            }
            if (toSend == null) {
                return;
            }
            for (GlobType globType : new HashSet<>(toSend)) {
                getAllSubType(globType, alreadySent, toSend);
            }
            rpcPersistence.declareType(typeName, gson.toJson(toSend.toArray(new GlobType[0])));
        }

        private void getAllSubType(GlobType globType, Map<String, GlobType> done, Collection<GlobType> types) {
            globType.streamAnnotations().map(Glob::getType)
                    .filter(type -> !done.containsKey(type.getName())).forEach(types::add);
            globType.streamFields().flatMap(Field::streamAnnotations).map(Glob::getType)
                    .filter(type -> !done.containsKey(type.getName())).forEach(types::add);
        }

        public List<Glob> list(GlobType type, Constraint constraint) {
            LOGGER.info("Call list" + type.getName());
            String constraintAsJson = gson.toJson(constraint);
            return Arrays.asList(gson.fromJson(rpcPersistence.list(typeName, type.getName(), constraintAsJson), Glob[].class));
        }

        public Glob getData(Glob tag) {
            LOGGER.info("Call getData");
            String dataAsJson = rpcPersistence.getData(typeName, gson.toJson(tag));
            return gson.fromJson(dataAsJson, Glob.class);
        }

        public Listener listen(GlobType type, Constraint constraint, OnChange consumer, GlobType[] additionalWantedTags) {
            LOGGER.info("Call listen");
            sendTypes(type, additionalWantedTags);
            String constraintAsJson = gson.toJson(constraint);
            String uuid = rpcPersistence.register(rpcListener.rpcListenerUUID, typeName, type.getName(), constraintAsJson, Arrays.stream(additionalWantedTags).map(GlobType::getName).toArray(String[]::new));
            DataRegister dataRegister = new DataRegister(uuid, consumer, gson);
            rpcListener.add(dataRegister);
            return new Listener() {
                public void unregister() {
                    rpcListener.remove(dataRegister);
                    rpcPersistence.unregister(rpcListener.rpcListenerUUID, uuid);
                }
            };
        }

        public void updateTag(Glob refTag, MutableGlob[] tags) {
            LOGGER.info("Call updateTag");
            sendTypes(refTag, tags);
            String uuid = rpcPersistence.updateTag(typeName, gson.toJson(refTag), gson.toJson(tags));
            for (MutableGlob tag : tags) {
                tag.set(getUUIDField(tag), uuid);
            }
        }

        public void stop() {
            //clean client only listener
        }
    }

    static class RpcListener {
        private final RpcPersistence rpcPersistence;
        private String rpcListenerUUID = UUID.randomUUID().toString();
        private Map<String, DataRegister> dataRegisters = new ConcurrentHashMap<>();
        private volatile boolean stoped = false;
        private long lastId = 0;

        RpcListener(RpcPersistence rpcPersistence) {
            this.rpcPersistence = rpcPersistence;
        }


        public void add(DataRegister dataRegister) {
            dataRegisters.put(dataRegister.uuid, dataRegister);
        }

        public void remove(DataRegister dataRegister) {
            dataRegisters.remove(dataRegister.uuid);
        }

        void stop() {
            stoped = true;
        }

        void run() {
            NanoChrono nanoChrono = NanoChrono.start();
            while (!stoped) {
                Ref<List<DataEvent>> events = new Ref<>();
                try {
                    lastId = rpcPersistence.getNextEvents(rpcListenerUUID, lastId, events);
                    List<DataEvent> dataEvents = events.get();
                    if (dataEvents != null) {
                        for (DataEvent dataEvent : dataEvents) {
                            callOnChange(dataEvent);
                        }
                    }
                    // to prevent to fast loop
                    if (nanoChrono.getElapsedTimeInMS() < 1000) {
                        try {
                            Thread.sleep((long) nanoChrono.getElapsedTimeInMS());
                        } catch (InterruptedException e) {
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("Rpc call fail, calling reset.", e);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                    }
                    for (DataRegister value : dataRegisters.values()) {
                        try {
                            value.onChange.reset();
                        } catch (Exception ex) {
                            LOGGER.error("Error on reset from "+ value.uuid, e);
                        }
                    }
                }
            }
        }

        private void callOnChange(DataEvent dataEvent) {
            try {
                DataRegister dataRegister = dataRegisters.get(dataEvent.listenerUUID);
                Gson gson = dataRegister.gson;
                Map<GlobType, Glob> additionalTags;
                if (dataEvent.additional != null && dataEvent.additional.length != 0) {
                    additionalTags = new HashMap<>();
                    for (String t : dataEvent.additional) {
                        Glob glob = gson.fromJson(t, Glob.class);
                        additionalTags.put(glob.getType(), glob);
                    }
                }
                else {
                    additionalTags = Collections.emptyMap();
                }
                dataRegister.onChange.change(gson.fromJson(dataEvent.oldValue, Glob.class),
                        gson.fromJson(dataEvent.newValue, Glob.class),additionalTags);
            } catch (Exception e) {
                LOGGER.error("on " + dataEvent.listenerUUID, e);
            }
        }
    }

    static class DataRegister {
        final String uuid;
        final ClientPersistence.OnChange onChange;
        final Gson gson;

        public DataRegister(String uuid, ClientPersistence.OnChange consumer, Gson gson) {
            this.uuid = uuid;
            onChange = consumer;
            this.gson = gson;
        }
    }

    public static StringField getUUIDField(Glob glob) {
        GlobType type = glob.getType();
        Field[] keyFields = type.getKeyFields();
        if (keyFields.length != 1) {
            String message = "Only one keyField expected for " + type.getName();
            LOGGER.error(message);
            throw new RuntimeException(message);
        }
        Field field = keyFields[0];
        if (field.getDataType() != DataType.String) {
            String message = "keyField must be a string " + type.getName();
            LOGGER.error(message);
            throw new RuntimeException(message);
        }
        return field.asStringField();
    }

}
