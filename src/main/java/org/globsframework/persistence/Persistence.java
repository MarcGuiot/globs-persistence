package org.globsframework.persistence;

import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.annotations.ArgType;
import org.globsframework.model.Glob;
import org.globsframework.model.MutableGlob;
import org.globsframework.sqlstreams.constraints.Constraint;

import java.util.List;

public interface Persistence {

    String pushData(Glob data, MutableGlob[] tags);

    List<Glob> list(GlobType type, Constraint constraint);

    Glob getData(Glob tag);

    Listener listen(GlobType type, Constraint constraint, OnChange consumer, GlobType[] additionalWantedTags);

    String updateTag(Glob tag, MutableGlob[] globs);

    void shutdown();

    interface OnChange {
        void change(GlobType type, Glob oldValue, Glob newValue, List<Glob> additionalTags);
    }

    interface Listener {
        void unregister();
    }

//    void addTag(Glob refTag, Glob newTag);
}
