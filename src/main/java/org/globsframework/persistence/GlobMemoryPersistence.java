package org.globsframework.persistence;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeBuilder;
import org.globsframework.metamodel.impl.DefaultGlobTypeBuilder;
import org.globsframework.metamodel.type.DataType;
import org.globsframework.model.Glob;
import org.globsframework.model.MutableGlob;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.constraints.ConstraintVisitor;
import org.globsframework.sqlstreams.constraints.OperandVisitor;
import org.globsframework.sqlstreams.constraints.impl.*;
import org.globsframework.utils.collections.MultiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * code non terminé: pour bench: a peu pret 2 fois plus rapide.
 */

public class GlobMemoryPersistence implements Persistence {
    private static Logger LOGGER = LoggerFactory.getLogger(GlobMemoryPersistence.class);
    private final RWDataAccess data;
    private final RWTagAccess tagAccess;
    private Map<String, Glob> tagsByUUID = new ConcurrentHashMap<>();
    private MultiMap<GlobType, ChangeDetector> changeDetectors = new MultiMap<>();
    Map<GlobType, PropagateTagData> tagsType = new ConcurrentHashMap<>();
    Map<Field, Field> tagFieldToTagInfoField = new ConcurrentHashMap<>();
    private GlobType tagType;

    public GlobMemoryPersistence(RWDataAccess data, RWTagAccess tagAccess) {
        this.data = data;
        this.tagAccess = tagAccess;

        tagAccess.listAll((uuid, tags) -> {
            while (tags.hasNext()) {
                Glob glob = tags.next();
//                globTypeGlobMap.put(glob.getType(), glob);
            }
            tagsByUUID.put(uuid, tagType.instantiate());
        });
    }

    interface PropagateTagData {
        void update(MutableGlob mutableGlob, Glob tag);
    }

    static class OneElementPropagateTagData implements PropagateTagData{
        private Field tagField;
        private Field tagInfoField;

        public OneElementPropagateTagData(Field tagField, Field tagInfoField) {
            this.tagField = tagField;
            this.tagInfoField = tagInfoField;
        }

        public void update(MutableGlob mutableGlob, Glob tag) {
            mutableGlob.setValue(tagInfoField, tag.getValue(tagField));
        }
    }

    public String pushData(Glob data, MutableGlob[] tags) {
        String key = this.data.save(data);

        boolean hasNewField = false;
        for (Glob tag : tags) {
            GlobType type = tag.getType();
            ((MutableGlob) tag).set(type.getKeyFields()[0].asStringField(), key);
            hasNewField |= !tagsType.containsKey(type);
        }

        tagAccess.save(key, tags);

        if (hasNewField) {
            renewTagTypeInfo(tags);
        }

        MutableGlob tagInfo = tagType.instantiate();
        for (Glob tag : tags) {
            GlobType type = tag.getType();
            tagsType.get(type).update(tagInfo, tag);
        }
        tagsByUUID.put(key, tagInfo);

        for (Glob tag : tags) {
            List<ChangeDetector> changeDetectors = this.changeDetectors.get(tag.getType());
            for (ChangeDetector changeDetector : changeDetectors) {
                changeDetector.callChange(tag.getType(), null, tag, tagInfo);
            }
        }
        return key;
    }

    private void renewTagTypeInfo(Glob[] tags) {
        GlobTypeBuilder globTypeBuilder = new DefaultGlobTypeBuilder("tags");
        for (Glob tag : tags) {
            for (Field f : tag.getType().getFields()) {
                if (!f.isKeyField()){
                    Field field = globTypeBuilder.declare(tag.getType().getName() + ":" + f.getName(), f.getDataType(), Collections.EMPTY_LIST);
                    tagsType.put(tag.getType(), new OneElementPropagateTagData(f, field));
                    tagFieldToTagInfoField.put(f, field);
                }
            }
        }
        tagType = globTypeBuilder.get();

        //il faut upgrader tout les globs.
    }

    public List<Glob> list(GlobType type, Constraint constraint) {
        List<Glob> result = new ArrayList<>();
        Filter filter = constraint.visit(new FilterConstraintVisitor(tagFieldToTagInfoField)).filter;
        for (Map.Entry<String, Glob> stringMapEntry : tagsByUUID.entrySet()) {
            if (filter.isEligible(stringMapEntry.getValue())) {
                // revert propagate info to tag.
                result.add(type.instantiate());
            }
        }
        return result;
    }

    public Glob getData(Glob tag) {
        String uuid = tag.get(tag.getType().getKeyFields()[0].asStringField());
        return data.getData(uuid);
    }

    public Listener listen(GlobType type, Constraint constraint, OnChange consumer, GlobType[] additionalWantedTags) {
        ChangeDetector value = new ChangeDetector(type, constraint.visit(new FilterConstraintVisitor(tagFieldToTagInfoField)).filter, consumer, additionalWantedTags);
        changeDetectors.put(type, value);
        return new Listener() {
            public void unregister() {
                changeDetectors.get(type).remove(value);
            }
        };
    }

    public String updateTag(Glob tag, MutableGlob[] globs) {
        String uuid = tag.get(tag.getType().getKeyFields()[0].asStringField());
        Glob globTypeGlobMap = tagsByUUID.get(uuid);
        return uuid;
//        Glob previousGlob = globTypeGlobMap.get(tag.getType());
//        List<ChangeDetector> changeDetectors = this.changeDetectors.get(tag.getType());
//        tagsByUUID.put(uuid, tag.getType(), tag);
//        for (ChangeDetector changeDetector : changeDetectors) {
//            changeDetector.callChange(tag.getType(), previousGlob, tag, globTypeGlobMap);
//        }
    }

    public void shutdown() {

    }

    interface Filter {
        boolean isEligible(Glob data);
    }

    interface DataAccess {
        Object getData(Glob data);
    }

    static class ChangeDetector {
        final private GlobType observedType;
        final private Filter filter;
        final private OnChange consumer;
        final private GlobType[] additionalWantedTags;

        public ChangeDetector(GlobType type, Filter filter, OnChange consumer, GlobType[] additionalWantedTags) {
            this.observedType = type;
            this.filter = filter;
            this.consumer = consumer;
            this.additionalWantedTags = additionalWantedTags == null || additionalWantedTags.length == 0? null : additionalWantedTags;
        }

        void callChange(GlobType globType, Glob oldTag, Glob newTag, Glob data) {
            if (globType == observedType && filter.isEligible(data)) {
                List<Glob> other;
                if (additionalWantedTags != null) {
                    throw new RuntimeException("TODO");
                }
                else {
                    other = Collections.EMPTY_LIST;
                }
                consumer.change(globType, oldTag, newTag, other);
            }
        }
    }

//    public void addTag(Glob refTag, Glob newTag) {
//
//    }

    private static class FilterConstraintVisitor implements ConstraintVisitor {
        Filter filter;
        private Map<Field, Field> tagFieldToTagInfoField;

        private FilterConstraintVisitor(Map<Field, Field> tagFieldToTagInfoField) {
            this.tagFieldToTagInfoField = tagFieldToTagInfoField;
        }

        public void visitEqual(EqualConstraint constraint) {
            ValueOperandVisitor valueOperandVisitor = constraint.getLeftOperand().visitOperand(new ValueOperandVisitor(tagFieldToTagInfoField));
            DataAccess leftAccess = valueOperandVisitor.value;
            DataAccess rightAccess = constraint.getRightOperand().visitOperand(new ValueOperandVisitor(tagFieldToTagInfoField)).value;
            filter = data -> valueOperandVisitor.field.valueEqual(leftAccess.getData(data), rightAccess.getData(data));
        }

        public void visitNotEqual(NotEqualConstraint constraint) {
            ValueOperandVisitor valueOperandVisitor = constraint.getLeftOperand().visitOperand(new ValueOperandVisitor(tagFieldToTagInfoField));
            DataAccess leftAccess = valueOperandVisitor.value;
            DataAccess rightAccess = constraint.getRightOperand().visitOperand(new ValueOperandVisitor(tagFieldToTagInfoField)).value;
            filter = data -> !valueOperandVisitor.field.valueEqual(leftAccess.getData(data), rightAccess.getData(data));
        }

        public void visitAnd(AndConstraint constraint) {
            Filter leftFilter = constraint.getLeftConstraint().visit(new FilterConstraintVisitor(tagFieldToTagInfoField)).filter;
            Filter rightFilter = constraint.getRightConstraint().visit(new FilterConstraintVisitor(tagFieldToTagInfoField)).filter;
            filter = data -> leftFilter.isEligible(data) && rightFilter.isEligible(data);
        }

        public void visitOr(OrConstraint constraint) {
            Filter leftFilter = constraint.getLeftConstraint().visit(new FilterConstraintVisitor(tagFieldToTagInfoField)).filter;
            Filter rightFilter = constraint.getRightConstraint().visit(new FilterConstraintVisitor(tagFieldToTagInfoField)).filter;
            filter = data -> leftFilter.isEligible(data) || rightFilter.isEligible(data);
        }

        public void visitLessThan(LessThanConstraint constraint) {
            ValueOperandVisitor leftVisitor = new ValueOperandVisitor(tagFieldToTagInfoField);
            constraint.getLeftOperand().visitOperand(leftVisitor);
            DataAccess leftAccess = leftVisitor.value;
            ValueOperandVisitor rightVisitor = new ValueOperandVisitor(tagFieldToTagInfoField);
            constraint.getRightOperand().visitOperand(rightVisitor);
            DataAccess rightAccess = rightVisitor.value;
            Field field = leftVisitor.field != null ? leftVisitor.field : rightVisitor.field;
            if (field != null) {
                DataType dataType = field.getDataType();
                if (dataType == DataType.DateTime) {
                    filter = data -> {
                        ZonedDateTime l = (ZonedDateTime) leftAccess.getData(data);
                        ZonedDateTime r = (ZonedDateTime) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.isBefore(r) || l.isEqual(r);
                        }
                        return false;
                    };
                } else if (dataType == DataType.Date) {
                    filter = data -> {
                        LocalDate l = (LocalDate) leftAccess.getData(data);
                        LocalDate r = (LocalDate) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.isBefore(r) || l.isEqual(r);
                        }
                        return false;
                    };
                } else if (dataType == DataType.Long || dataType == DataType.Double || dataType == DataType.Integer || dataType == DataType.BigDecimal) {
                    filter = data -> {
                        Comparable l = (Comparable) leftAccess.getData(data);
                        Comparable r = (Comparable) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.compareTo(r) <= 0;
                        }
                        return false;
                    };
                } else {
                    filter = data -> false;
                }
            }
        }

        public void visitBiggerThan(BiggerThanConstraint constraint) {
            ValueOperandVisitor leftVisitor = new ValueOperandVisitor(tagFieldToTagInfoField);
            constraint.getLeftOperand().visitOperand(leftVisitor);
            DataAccess leftAccess = leftVisitor.value;
            ValueOperandVisitor rightVisitor = new ValueOperandVisitor(tagFieldToTagInfoField);
            constraint.getRightOperand().visitOperand(rightVisitor);
            DataAccess rightAccess = rightVisitor.value;
            Field field = leftVisitor.field != null ? leftVisitor.field : rightVisitor.field;
            if (field != null) {
                DataType dataType = field.getDataType();
                if (dataType == DataType.DateTime) {
                    filter = data -> {
                        ZonedDateTime l = (ZonedDateTime) leftAccess.getData(data);
                        ZonedDateTime r = (ZonedDateTime) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.isAfter(r) || l.isEqual(r);
                        }
                        return false;
                    };
                } else if (dataType == DataType.Date) {
                    filter = data -> {
                        LocalDate l = (LocalDate) leftAccess.getData(data);
                        LocalDate r = (LocalDate) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.isAfter(r) || l.isEqual(r);
                        }
                        return false;
                    };
                } else if (dataType == DataType.Long || dataType == DataType.Double || dataType == DataType.Integer || dataType == DataType.BigDecimal) {
                    filter = data -> {
                        Comparable l = (Comparable) leftAccess.getData(data);
                        Comparable r = (Comparable) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.compareTo(r) >= 0;
                        }
                        return false;
                    };
                } else {
                    filter = data -> false;
                }
            }

        }

        public void visitStrictlyBiggerThan(StrictlyBiggerThanConstraint constraint) {
            ValueOperandVisitor leftVisitor = new ValueOperandVisitor(tagFieldToTagInfoField);
            constraint.getLeftOperand().visitOperand(leftVisitor);
            DataAccess leftAccess = leftVisitor.value;
            ValueOperandVisitor rightVisitor = new ValueOperandVisitor(tagFieldToTagInfoField);
            constraint.getRightOperand().visitOperand(rightVisitor);
            DataAccess rightAccess = rightVisitor.value;
            Field field = leftVisitor.field != null ? leftVisitor.field : rightVisitor.field;
            if (field != null) {
                DataType dataType = field.getDataType();
                if (dataType == DataType.DateTime) {
                    filter = data -> {
                        ZonedDateTime l = (ZonedDateTime) leftAccess.getData(data);
                        ZonedDateTime r = (ZonedDateTime) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.isAfter(r);
                        }
                        return false;
                    };
                } else if (dataType == DataType.Date) {
                    filter = data -> {
                        LocalDate l = (LocalDate) leftAccess.getData(data);
                        LocalDate r = (LocalDate) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.isAfter(r);
                        }
                        return false;
                    };
                } else if (dataType == DataType.Long || dataType == DataType.Double || dataType == DataType.Integer || dataType == DataType.BigDecimal) {
                    filter = data -> {
                        Comparable l = (Comparable) leftAccess.getData(data);
                        Comparable r = (Comparable) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.compareTo(r) > 0;
                        }
                        return false;
                    };
                } else {
                    filter = data -> false;
                }
            }

        }

        public void visitStrictlyLesserThan(StrictlyLesserThanConstraint constraint) {
            ValueOperandVisitor leftVisitor = new ValueOperandVisitor(tagFieldToTagInfoField);
            constraint.getLeftOperand().visitOperand(leftVisitor);
            DataAccess leftAccess = leftVisitor.value;
            ValueOperandVisitor rightVisitor = new ValueOperandVisitor(tagFieldToTagInfoField);
            constraint.getRightOperand().visitOperand(rightVisitor);
            DataAccess rightAccess = rightVisitor.value;
            Field field = leftVisitor.field != null ? leftVisitor.field : rightVisitor.field;
            if (field != null) {
                DataType dataType = field.getDataType();
                if (dataType == DataType.DateTime) {
                    filter = data -> {
                        ZonedDateTime l = (ZonedDateTime) leftAccess.getData(data);
                        ZonedDateTime r = (ZonedDateTime) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.isBefore(r);
                        }
                        return false;
                    };
                } else if (dataType == DataType.Date) {
                    filter = data -> {
                        LocalDate l = (LocalDate) leftAccess.getData(data);
                        LocalDate r = (LocalDate) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.isBefore(r);
                        }
                        return false;
                    };
                } else if (dataType == DataType.Long || dataType == DataType.Double || dataType == DataType.Integer || dataType == DataType.BigDecimal) {
                    filter = data -> {
                        Comparable l = (Comparable) leftAccess.getData(data);
                        Comparable r = (Comparable) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.compareTo(r) < 0;
                        }
                        return false;
                    };
                } else {
                    filter = data -> false;
                }
            }

        }

        public void visitIn(InConstraint constraint) {

        }

        public void visitIsOrNotNull(NullOrNotConstraint constraint) {

        }

        public void visitNotIn(NotInConstraint constraint) {

        }

        public void visitContains(Field field, String value, boolean contains, boolean startWith, boolean ignoreCase) {

        }

        private static class ValueOperandVisitor implements OperandVisitor {
            DataAccess value;
            Field field;
            private Map<Field, Field> tagFieldToTagInfoField;

            public ValueOperandVisitor(Map<Field, Field> tagFieldToTagInfoField) {
                this.tagFieldToTagInfoField = tagFieldToTagInfoField;
            }

            public void visitValueOperand(ValueOperand value) {
                this.value = data -> value.getValue();
                this.field = value.getField();
            }

            public void visitAccessorOperand(AccessorOperand accessorOperand) {
                this.value = data -> accessorOperand.getAccessor().getObjectValue();
                this.field = accessorOperand.getField();
            }

            public void visitFieldOperand(Field field) {
                this.field = field;
                Field f = tagFieldToTagInfoField.get(field);
                this.value = data -> data.getValue(f);
            }
        }
    }
}
