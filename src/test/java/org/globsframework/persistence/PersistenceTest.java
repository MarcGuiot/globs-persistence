package org.globsframework.persistence;

import org.globsframework.metamodel.GlobModel;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.FieldNameAnnotation;
import org.globsframework.metamodel.annotations.KeyField;
import org.globsframework.metamodel.annotations.Target;
import org.globsframework.metamodel.fields.BooleanField;
import org.globsframework.metamodel.fields.DoubleField;
import org.globsframework.metamodel.fields.GlobArrayField;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.metamodel.impl.DefaultGlobModel;
import org.globsframework.model.Glob;
import org.globsframework.model.MutableGlob;
import org.globsframework.sqlstreams.constraints.Constraints;
import org.globsframework.utils.NanoChrono;
import org.globsframework.utils.collections.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;


public class PersistenceTest {

    public static GlobModel globTypes = new DefaultGlobModel(OwnerTagInfo.TYPE, PriceRuleTagInfo.TYPE, PriceStatusTagInfo.TYPE, PriceRule.TYPE);


    // inmutable ==> ok mais on peut creer plusieurs object "identique" et prendre leur dernier état.
    // le systeme doit permettre de récuperer le dernier état.
    // si un object a une clée, cela signifie qu'il existe en plusieurs version ==> comment gérer les contraintes si on fait ca?
    // pourquoi faire?


    // pour une meme data on peut avoir plusieurs tag identique
    // Si on a une donnée en erreur, il faut en mettre une nouvelle, elle aura les meme tag.
    // soit cette donnée sera acccesible commes les autres sauf un tag qui dit check ok/ko => valable pour des flux automatiques

    // dans un second temps:
    // soit la nouvelle donnée "cache" la précédente => il faut une clée fonctionnel pour dire que deux jeux de donnés ont la meme fin => import nommé.

    //

    @Test
    public void name() throws InterruptedException {
        Persistence persistence = new InMemoryPersistence(new InMemoryRWDataAccess(), new NullRWTagAccess());

        Glob priceRule = PriceRule.TYPE.instantiate()
                .set(PriceRule.name, "VP 04-2020 D1")
                .set(PriceRule.price, new Glob[]{PriceBySku.TYPE.instantiate().set(PriceBySku.sku, "ARS").set(PriceBySku.value, 3.14)});

        MutableGlob owner = OwnerTagInfo.TYPE.instantiate()
                .set(OwnerTagInfo.NAME, "Pablo");

        MutableGlob type = PriceRuleTagInfo.TYPE.instantiate()
                .set(PriceRuleTagInfo.name, "VP 04-2020 D1");

        MutableGlob status = PriceStatusTagInfo.TYPE.instantiate().set(PriceStatusTagInfo.isEnable, false);

        persistence.pushData(priceRule.duplicate(), new MutableGlob[]{owner, type, status});

        List<Glob> priceRules = persistence.list(PriceRuleTagInfo.TYPE,
                Constraints.and(
                        Constraints.equal(OwnerTagInfo.NAME, "Pablo"),
                        Constraints.equal(PriceStatusTagInfo.isEnable, false)));

        Glob data = persistence.getData(priceRules.get(0));
        Assert.assertNotNull(data);

        BlockingQueue<Pair<Glob, List<Glob>>> globs = new LinkedBlockingDeque<>();
        persistence.listen(PriceStatusTagInfo.TYPE,
                Constraints.equal(OwnerTagInfo.NAME, "Pablo"), new Persistence.OnChange() {
                    public void change(GlobType type, Glob oldValue, Glob newValue, List<Glob> additionalTags) {
                        globs.add(Pair.makePair(newValue, additionalTags));
                    }
                }, new GlobType[]{PriceRuleTagInfo.TYPE});

        persistence.updateTag(status, new MutableGlob[]{status.duplicate().set(PriceStatusTagInfo.isEnable, true)});

        Pair<Glob, List<Glob>> poll = globs.poll(1, TimeUnit.SECONDS);

        Assert.assertNotNull(poll);
        Assert.assertEquals("VP 04-2020 D1", poll.getSecond().get(0).get(PriceRuleTagInfo.name));
    }


    @Test
    public void bench() {
        Persistence persistence = new GlobMemoryPersistence(new InMemoryRWDataAccess(), new NullRWTagAccess());
//        Persistence persistence = new InMemoryPersistence(new InMemoryRWDataAccess(), new NullRWTagAccess());

        for (int i = 0; i < 100000; i++) {
            Glob priceRule = PriceRule.TYPE.instantiate()
                    .set(PriceRule.name, "VP 04-2020 D" + i)
                    .set(PriceRule.price, new Glob[]{PriceBySku.TYPE.instantiate().set(PriceBySku.sku, "ARS").set(PriceBySku.value, 3.14)});

            MutableGlob owner = OwnerTagInfo.TYPE.instantiate()
                    .set(OwnerTagInfo.NAME, "Pablo");

            MutableGlob type = PriceRuleTagInfo.TYPE.instantiate()
                    .set(PriceRuleTagInfo.name, "VP 04-2020 D" + i);

            MutableGlob status = PriceStatusTagInfo.TYPE.instantiate().set(PriceStatusTagInfo.isEnable, (i % 2) == 0);

            persistence.pushData(priceRule, new MutableGlob[]{owner, type, status});
        }

        System.out.println("PersistenceTest.bench");
        NanoChrono nanoChrono = NanoChrono.start();
        for (int i = 0; i < 100; i++) {
            List<Glob> priceRules = persistence.list(PriceRuleTagInfo.TYPE,
                    Constraints.and(
                            Constraints.equal(OwnerTagInfo.NAME, "Pablo"),
                            Constraints.equal(PriceStatusTagInfo.isEnable, true)));
            Assert.assertFalse(priceRules.isEmpty());
        }
        System.out.println("PersistenceTest.bench " + nanoChrono.getElapsedTimeInMS());

    }

    static public class OwnerTagInfo {
        public static GlobType TYPE;

        @KeyField
        public static StringField UUID;

        public static StringField NAME;

        static {
            GlobTypeLoaderFactory.create(OwnerTagInfo.class).load();
        }
    }

    static public class PriceRuleTagInfo {
        public static GlobType TYPE;

        @KeyField
        public static StringField UUID;

        public static StringField name;

        static {
            GlobTypeLoaderFactory.create(PriceRuleTagInfo.class).load();
        }
    }

    static public class PriceStatusTagInfo {
        public static GlobType TYPE;

        @KeyField
        public static StringField UUID;

        @FieldNameAnnotation("isEnable")
        public static BooleanField isEnable;

        static {
            GlobTypeLoaderFactory.create(PriceStatusTagInfo.class).load();
        }
    }

    static public class PriceCheckTagInfo {
        public static GlobType TYPE;

        @KeyField
        public static StringField UUID;

        public static StringField source;

        public static BooleanField isEnable;

        static {
            GlobTypeLoaderFactory.create(PriceStatusTagInfo.class).load();
        }
    }

    static public class PriceBySku {
        public static GlobType TYPE;

        public static StringField sku;

        public static DoubleField value;

        static {
            GlobTypeLoaderFactory.create(PriceBySku.class).load();
        }
    }

    static public class PriceRule {
        public static GlobType TYPE;

        public static StringField name;

        @Target(PriceBySku.class)
        public static GlobArrayField price;

        static {
            GlobTypeLoaderFactory.create(PriceRule.class).load();
        }
    }

    private static class InMemoryRWDataAccess implements RWDataAccess {
        Map<String, Glob> data = new ConcurrentHashMap<>();

        public Glob getData(String uuid) {
            return data.get(uuid);
        }

        public String save(Glob glob) {
            String uuid;
            do {
                uuid = UUID.randomUUID().toString();
            } while (data.containsKey(uuid));
            data.put(uuid, glob);
            return uuid;
        }

        public void shutdown() {

        }
    }

    private static class NullRWTagAccess implements RWTagAccess {
        public void save(String uuid, Glob[] tags) {

        }

        public void listAll(TagListener consumer) {

        }

        public void shutdown() {

        }
    }
}