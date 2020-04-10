package org.globsframework.persistence.http.server;

import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.KeyField;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.metamodel.impl.DefaultGlobModel;
import org.globsframework.model.Glob;
import org.globsframework.model.MutableGlob;
import org.globsframework.persistence.http.client.ClientPersistence;
import org.globsframework.persistence.http.client.DefaultPersistenceRemoteClient;
import org.globsframework.persistence.http.client.PersistenceRemoteClient;
import org.globsframework.remote.shared.ServerSharedData;
import org.globsframework.remote.shared.impl.DefaultSharedDataManager;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class PersistenceHttpServerTest {

    @Test
    public void name() throws IOException, InterruptedException {
        MutableGlob instantiate = PersistenceHttpServer.Options.TYPE.instantiate();
        ServerSharedData serverSharedData = DefaultSharedDataManager.initSharedData();
        Path rootDir = Files.createTempDirectory("rpcPersistenceTest");
        instantiate.set(PersistenceHttpServer.Options.rootDirectory, rootDir.toFile().getAbsolutePath())
                .set(PersistenceHttpServer.Options.host, serverSharedData.getHost())
                .set(PersistenceHttpServer.Options.port, serverSharedData.getPort())
                .set(PersistenceHttpServer.Options.globTypeName, SomeData.TYPE.getName())
        ;
        PersistenceHttpServer persistenceHttpServer = new PersistenceHttpServer(instantiate);

        DefaultGlobModel globModel = new DefaultGlobModel();
        org.globsframework.metamodel.annotations.AllAnnotations.MODEL.forEach(globModel::add);
        org.globsframework.sqlstreams.annotations.AllAnnotations.MODEL.forEach(globModel::add);
        org.globsframework.json.annottations.AllAnnotations.MODEL.forEach(globModel::add);
        PersistenceRemoteClient persistenceRemoteClient = new DefaultPersistenceRemoteClient(serverSharedData.getHost(), serverSharedData.getPort());
        persistenceRemoteClient.createClient(SomeData.TYPE, globModel::findType);

        ClientPersistence clientPersistence = persistenceRemoteClient.get(SomeData.TYPE);

        BlockingQueue<Received> receiveds = new LinkedBlockingDeque<>();

        clientPersistence.listen(Tag1.TYPE, null, new ClientPersistence.OnChange() {
            public void change(Glob oldValue, Glob newValue, Map<GlobType, Glob> additionalTags) {
                receiveds.add(new Received(oldValue, newValue, additionalTags));
            }

            public void reset() {

            }
        }, new GlobType[]{Tag2.TYPE});

        MutableGlob ttt1 = Tag1.TYPE.instantiate().set(Tag1.data, "TTT1");
        clientPersistence.pushData(SomeData.TYPE.instantiate().set(SomeData.data, "AAAA"),
                new MutableGlob[]{ttt1});

        Received poll = receiveds.poll(1, TimeUnit.MINUTES);
        Assert.assertNotNull(poll);

        clientPersistence.updateTag(ttt1,
                new MutableGlob[]{
                        Tag2.TYPE.instantiate().set(Tag2.data, "TTT2"),
                        ttt1.duplicate().set(Tag1.data, "T")});

        Received poll2 = receiveds.poll(1, TimeUnit.MINUTES);
        Assert.assertNotNull(poll2);
        Assert.assertEquals("TTT2", poll2.additionalTags.get(Tag2.TYPE).get(Tag2.data));

        clientPersistence.stop();
        persistenceHttpServer.shutDown();
        serverSharedData.stop();
    }

    static public class Received {

        private final Glob oldValue;
        private final Glob newValue;
        private final Map<GlobType, Glob> additionalTags;

        public Received(Glob oldValue, Glob newValue, Map<GlobType, Glob> additionalTags) {
            this.oldValue = oldValue;
            this.newValue = newValue;
            this.additionalTags = additionalTags;
        }
    }

    static public class SomeData {
        public static GlobType TYPE;

        @KeyField
        public static StringField UUID;

        public static StringField data;

        static {
            GlobTypeLoaderFactory.create(SomeData.class).load();
        }
    }

    static public class Tag1 {
        public static GlobType TYPE;

        @KeyField
        public static StringField UUID;

        public static StringField data;

        static {
            GlobTypeLoaderFactory.create(Tag1.class).load();
        }
    }

    static public class Tag2 {
        public static GlobType TYPE;

        @KeyField
        public static StringField UUID;

        public static StringField data;

        static {
            GlobTypeLoaderFactory.create(Tag2.class).load();
        }
    }
}