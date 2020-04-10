package org.globsframework.persistence.file;

import org.globsframework.model.Glob;
import org.globsframework.persistence.PersistenceTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class FileRWDataAccessTest {

    @Test
    public void name() throws IOException {
        Path tempFile = Files.createTempDirectory("testRWAccessTag");
        System.out.println("FileRWTagAccessTest.name: creating dir " + tempFile.toString());

        FileRWDataAccess fileRWDataAccess = new FileRWDataAccess(tempFile.toFile(), PersistenceTest.globTypes::getType);
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            String uuid = fileRWDataAccess.save(PersistenceTest.PriceRule.TYPE.instantiate().set(PersistenceTest.PriceRule.name, "VP " + i)
                    .set(PersistenceTest.PriceRule.price, new Glob[]{PersistenceTest.PriceBySku.TYPE.instantiate().set(PersistenceTest.PriceBySku.sku, "sku")
                            .set(PersistenceTest.PriceBySku.value, 3.1415)}));
            map.put(uuid, i);
        }

        for (Map.Entry<String, Integer> s : map.entrySet()) {
            Glob data = fileRWDataAccess.getData(s.getKey());
            Assert.assertNotNull(data);
            Assert.assertEquals(data.get(PersistenceTest.PriceRule.name), "VP " + s.getValue());
        }
        org.globsframework.utils.Files.deleteWithSubtree(tempFile.toFile());
    }
}