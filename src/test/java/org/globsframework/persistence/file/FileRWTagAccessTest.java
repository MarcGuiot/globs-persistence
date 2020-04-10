package org.globsframework.persistence.file;

import org.globsframework.model.Glob;
import org.globsframework.persistence.PersistenceTest;
import org.globsframework.persistence.RWTagAccess;
import org.globsframework.utils.collections.MultiMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class FileRWTagAccessTest {


    @Test
    public void name() throws IOException {
        Path tempFile = Files.createTempDirectory("testRWAccessTag");
        System.out.println("FileRWTagAccessTest.name: creating dir " + tempFile.toString());
        MultiMap<String, Glob> tags = new MultiMap<>();
        {
            FileRWTagAccess fileRWTagAccess =
                    new FileRWTagAccess(tempFile, PersistenceTest.globTypes::getType);

            for (int i = 0; i < 10000; i++) {
                String uuid = UUID.randomUUID().toString();
                Glob[] tags1 = {
                        PersistenceTest.OwnerTagInfo.TYPE.instantiate().set(PersistenceTest.OwnerTagInfo.UUID, uuid).set(PersistenceTest.OwnerTagInfo.NAME, "name " + i),
                        PersistenceTest.PriceStatusTagInfo.TYPE.instantiate().set(PersistenceTest.PriceStatusTagInfo.UUID, uuid).set(PersistenceTest.PriceStatusTagInfo.isEnable, false)
                };
                tags.putAll(uuid, Arrays.asList(tags1));
                fileRWTagAccess.save(uuid, tags1);
            }
        }
        {
            FileRWTagAccess fileRWTagAccess =
                    new FileRWTagAccess(tempFile, PersistenceTest.globTypes::getType);
            fileRWTagAccess.listAll(new RWTagAccess.TagListener() {
                public void accept(String uuid, Iterator<Glob> t) {
                    List<Glob> globs = new ArrayList<>(tags.remove(uuid));
                    Assert.assertFalse(globs.isEmpty());
                    while (t.hasNext()) {
                        Glob next = t.next();
                        Assert.assertTrue(globs.remove(next));
                    }
                    Assert.assertTrue(globs.isEmpty());
                }
            });
            Assert.assertTrue(tags.isEmpty());
        }
        org.globsframework.utils.Files.deleteWithSubtree(tempFile.toFile());
    }
}