package com.stephen;

import com.stephen.exception.RaftErrorException;
import eraftpb.Eraftpb;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.stephen.lang.Vec;

public class $ {

    /// Check whether the entry is continuous to the message.
    /// i.e msg's next entry index should be equal to the first entries's index
    public static boolean isContinuousEntries(Eraftpb.Message msg, List<Eraftpb.Entry> entries) {
        var msgEntries = msg.getEntriesList();
        if (isNotEmpty(msgEntries) && isNotEmpty(entries)) {
            var expectedNextIdx = msgEntries.get(msgEntries.size() - 1).getIndex() + 1;
            return expectedNextIdx == entries.get(0).getIndex();
        }
        return true;
    }

    public static Long limitSize(Vec<Eraftpb.Entry> entries, Long max) {
        if (entries.size() <= 1 || max == null) {
            return null;
        }
        AtomicLong size = new AtomicLong(0);
        long limit = entries.stream()
                .takeWhile(e -> size.addAndGet(e.getSerializedSize()) <= max)
                .count();
        entries.truncate(limit);
        return limit;
    }

    public static <T> T unwrap(CheckExceptionFunction<T>  supplier, T defaultValue) {
        try {
            return supplier.get();
        } catch (RaftErrorException e) {
            return defaultValue;
        }
    }

    public static <VALUE> boolean isEmpty(Collection<VALUE> collection) {
        return collection == null || collection.isEmpty();
    }

    public static <VALUE> boolean isNotEmpty(Collection<VALUE> collection) {
        return !isEmpty(collection);
    }

}
