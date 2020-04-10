package org.globsframework.persistence.http.server;

import org.apache.http.ExceptionLogger;
import org.globsframework.commandline.Mandatory_;
import org.globsframework.commandline.ParseCommandLine;
import org.globsframework.directory.DefaultDirectory;
import org.globsframework.directory.Directory;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.FieldNameAnnotation;
import org.globsframework.metamodel.fields.IntegerField;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.model.Glob;
import org.globsframework.persistence.Persistence;
import org.globsframework.persistence.http.DataEvent;
import org.globsframework.persistence.http.RpcPersistence;
import org.globsframework.remote.DirectoryProvider;
import org.globsframework.remote.peer.direct.DirectPeerToPeer;
import org.globsframework.remote.rpc.RpcService;
import org.globsframework.remote.rpc.impl.DefaultRpcService;
import org.globsframework.remote.shared.AddressAccessor;
import org.globsframework.remote.shared.SharedDataManager;
import org.globsframework.remote.shared.impl.DefaultSharedDataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class PersistenceHttpServer {
    public static final String SERVICE_NAME = "persistence";
    public static final String UNIQUE = "unique";
    public static Logger LOGGER = LoggerFactory.getLogger(PersistenceHttpServer.class);
    private final Directory directory;
    private final DirectPeerToPeer peerToPeer;
    private final SharedDataManager sharedDataManager;
    private final RpcService rpcService;
    private final PersistenceProviderImpl persistenceProvider;
    private boolean shutDown = false;

    public PersistenceHttpServer(Glob option) {
        directory = new DefaultDirectory();
        peerToPeer = new DirectPeerToPeer();
        sharedDataManager = DefaultSharedDataManager.create(AddressAccessor.FixAddressAccessor
                .create(option.get(Options.host), option.get(Options.port)));
        DefaultRpcService.registerRpcNamingServiceHere(sharedDataManager);
        rpcService = new DefaultRpcService("worker", new DirectoryProvider() {
            public Directory getDirectory() {
                return directory;
            }
        }, sharedDataManager, peerToPeer);
        persistenceProvider = new PersistenceProviderImpl(new File(option.get(Options.rootDirectory)).toPath());
        rpcService.register(RpcPersistence.class, new RpcPersistenceImpl(
                persistenceProvider), UNIQUE, new DataEvent.EventsSerializer());

    }

    public void shutDown() {
        persistenceProvider.shutdown();
        rpcService.reset();
        peerToPeer.destroy();
        synchronized (this) {
            shutDown = true;
            notifyAll();
        }
    }


    private void waitShutdown() throws InterruptedException {
        synchronized (this) {
            while (!shutDown) {
                wait();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Glob option = ParseCommandLine.parse(Options.TYPE, args);
        PersistenceHttpServer persistenceHttpServer = new PersistenceHttpServer(option);

        persistenceHttpServer.waitShutdown();

//        final SharedDataService dataService = new ClientSharedData(globModel,
//                AddressAccessor.FixAddressAccessor.create(option.get(Options.host), option.get(Options.port)), ClientSharedData.OnStop.NULL, "/");


//        final IOReactorConfig config = IOReactorConfig.custom()
//                .setSoReuseAddress(true)
//                .setSoTimeout(15000)
//                .setTcpNoDelay(true)
//                .build();
//
//        ServerBootstrap serverBootstrap = ServerBootstrap.bootstrap()
//                .setListenerPort(0)
//                .setServerInfo("PersistenceServer/1.1")
//                .setIOReactorConfig(config)
//                .setExceptionLogger(new StdErrorExceptionLogger());
//
//        HttpServerRegister httpServerRegister = new HttpServerRegister(serverBootstrap);
//
//        httpServerRegister.register("/persistence/push", null)
//        .post();
//
//        dataService.write(new SharedDataService.SharedData() {
//            public void data(GlobRepository globRepository) {
//                globRepository.create(HttpServerLocation.TYPE,
//                        FieldValue.value(HttpServerLocation.SHARED_ID, dataService.getId()),
//                        FieldValue.value(HttpServerLocation.SERVICE, SERVICE_NAME),
//                        FieldValue.value(HttpServerLocation.PORT, )
//                );
//            }
//        });
    }


    static public class Options {
        public static GlobType TYPE;

        @FieldNameAnnotation("globTypeName")
        @Mandatory_
        public static StringField globTypeName;

        @FieldNameAnnotation("host")
        @Mandatory_
        public static StringField host;

        @FieldNameAnnotation("port")
        @Mandatory_
        public static IntegerField port;

        @FieldNameAnnotation("rootDirectory")
        @Mandatory_
        public static StringField rootDirectory;


        static {
            GlobTypeLoaderFactory.create(Options.class).load();
        }
    }

    private static class StdErrorExceptionLogger implements ExceptionLogger {

        public void log(Exception e) {
            LOGGER.error("error in http layer", e);
        }
    }

    static class PerClientInfo {
        public final String uuid;
        private final PersistenceProvider.PersistenceInfo persistenceInfo;
        private final Map<String, Persistence.Listener> listeners = new ConcurrentHashMap<>();
        public long lastUpdateId;
        public long lastRpcCall;
        public BlockingQueue<DataEvent> dataEvents = new LinkedBlockingDeque<>();

        public PerClientInfo(String uuid, PersistenceProvider.PersistenceInfo persistenceInfo) {
            this.uuid = uuid;
            this.persistenceInfo = persistenceInfo;
        }

        public void onChange(String uuid, GlobType type, Glob oldValue, Glob newValue, List<Glob> additionalTags) {
            dataEvents.add(new DataEvent(uuid, persistenceInfo.gson.toJson(oldValue),
                    persistenceInfo.gson.toJson(newValue),
                    additionalTags.stream().map(persistenceInfo.gson::toJson).toArray(String[]::new)));
        }

        public void register(String uuid, Persistence.Listener listen) {
            listeners.put(uuid, listen);
        }

        public void unregister(String uuid) {
            Persistence.Listener remove = listeners.remove(uuid);
            if (remove != null) {
                remove.unregister();
            }
        }

        public List<DataEvent> drain() {
            DataEvent poll = null;
            try {
                poll = dataEvents.poll(1, TimeUnit.MINUTES);
                if (poll == null) {
                    return Collections.emptyList();
                }
                List<DataEvent> events = new ArrayList<>();
                events.add(poll);
                dataEvents.drainTo(events);
                return events;
            } catch (Exception e) {
                LOGGER.error("in drain", e);
            }
            return Collections.emptyList();
        }
    }
}
