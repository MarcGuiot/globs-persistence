package org.globsframework.persistence.file;

import junit.framework.Assert;
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
            Assert.assertNotNull(typeAccess.find(PersistenceTest.OwnerTagInfo.TYPE.getName()));
        }
        {
            FileGlobTypeAccess typeAccess = new DefaultFileGlobTypeAccess(tempFile);
            Assert.assertNotNull(typeAccess.find(PersistenceTest.OwnerTagInfo.TYPE.getName()));
            Assert.assertNotNull(typeAccess.find(PersistenceTest.PriceRuleTagInfo.TYPE.getName()));
        }
        org.globsframework.utils.Files.deleteWithSubtree(tempFile.toFile());
    }
}