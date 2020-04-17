package org.globsframework.commandline;

import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.DefaultInteger;
import org.globsframework.metamodel.fields.IntegerField;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.model.Glob;
import org.junit.Assert;
import org.junit.Test;

public class ParseCommandLineTest {

    @Test
    public void name() {
        Glob opt = ParseCommandLine.parse(Opt1.TYPE, new String[]{"--name", "a name"});
        Assert.assertEquals("a name", opt.get(Opt1.NAME));
        Assert.assertEquals(123, opt.get(Opt1.VAL).intValue());

        String[] args = {"--name", "a name", "--val", "321"};
        opt = ParseCommandLine.parse(Opt1.TYPE, args);
        Assert.assertEquals("a name", opt.get(Opt1.NAME));
        Assert.assertEquals(321, opt.get(Opt1.VAL).intValue());
        Assert.assertArrayEquals(args, ParseCommandLine.toArgs(opt));
    }



    public static class Opt1 {
        public static GlobType TYPE;

        public static StringField NAME;

        @DefaultInteger(123)
        public static IntegerField VAL;

        static {
            GlobTypeLoaderFactory.create(Opt1.class).load();
        }
    }
}