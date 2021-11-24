package org.globsframework.persistence.file;

import junit.framework.Assert;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.Target;
import org.globsframework.metamodel.fields.GlobField;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.persistence.FileGlobTypeAccess;
import org.globsframework.persistence.PersistenceTest;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DefaultFileGlobTypeAccessTest {

    @Test
    public void name() throws IOException {
        Path tempFile = Files.createTempDirectory("testFileGobType");
        System.out.println("DefaultFileGlobTypeAccessTest.name: creating dir " + tempFile.toString());

        {
            FileGlobTypeAccess typeAccess = new DefaultFileGlobTypeAccess(tempFile);
            typeAccess.declare(PersistenceTest.OwnerTagInfo.TYPE);
            typeAccess.declare(PersistenceTest.PriceRuleTagInfo.TYPE);
            typeAccess.declare(Type1.TYPE);
            Assert.assertNotNull(typeAccess.find(PersistenceTest.OwnerTagInfo.TYPE.getName()));
        }
        {
            FileGlobTypeAccess typeAccess = new DefaultFileGlobTypeAccess(tempFile);
            Assert.assertNotNull(typeAccess.find(PersistenceTest.OwnerTagInfo.TYPE.getName()));
            Assert.assertNotNull(typeAccess.find(PersistenceTest.PriceRuleTagInfo.TYPE.getName()));
            Assert.assertNotNull(typeAccess.find(Type1.TYPE.getName()));
        }
        org.globsframework.utils.Files.deleteWithSubtree(tempFile.toFile());
    }

    public static class Type1{
        public static GlobType TYPE;

        @Target(Type2.class)
        public static GlobField child;

        static {
            GlobTypeLoaderFactory.create(Type1.class).load();
        }
    }

    public static class Type2{
        public static GlobType TYPE;

        public static StringField name;

        static {
            GlobTypeLoaderFactory.create(Type2.class).load();
        }
    }

}