/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.memory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.processor.AbstractTransaction;

/**
 * A concrete implementation of a {@link AbstractTransaction} for the JVM heap memory processor.
 *
 * @author Chris Cranford
 */
public class MemoryTransaction extends AbstractTransaction {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryTransaction.class);

    private int numberOfEvents;
    private List<LogMinerEvent> events;

    private final Set<String> rollbackEvents = new HashSet<>();

    public MemoryTransaction(String transactionId, Scn startScn, Instant changeTime, String userName) {
        super(transactionId, startScn, changeTime, userName);
        this.events = new ArrayList<>();
        start();
    }

    @Override
    public int getNumberOfEvents() {
        return numberOfEvents;
    }

    @Override
    public int getNextEventId() {
        return numberOfEvents++;
    }

    @Override
    public void start() {
        numberOfEvents = 0;
    }

    public List<LogMinerEvent> getEvents() {
        return events;
    }

    public boolean removeEventWithRow(LogMinerEventRow row) {
        String uniqueKey = row.getRsId() + ":" + row.getSsn();
        if (rollbackEvents.contains(uniqueKey)) {
            LOGGER.warn("{} already rollback.", uniqueKey);
            return false;
        }
        else {
            boolean remove = removeEventWithRowId(row.getRowId());
            if (remove) {
                rollbackEvents.add(uniqueKey);
            }
            return remove;
        }
    }

    public boolean removeEventWithRowId(String rowId) {
        // return events.removeIf(event -> {
        // if (event.getRowId().equals(rowId)) {
        // LOGGER.trace("Undo applied for event {}.", event);
        // return true;
        // }
        // return false;
        // });
        int size = events.size();
        int idx = size - 1;
        boolean remove = false;
        if (size > 0) {
            for (int i = size - 1; i >= 0; i--) {
                LogMinerEvent event = events.get(i);
                if (event.getRowId().equals(rowId)) {
                    LOGGER.debug("Undo applied for event {}.", event);
                    idx = i;
                    remove = true;
                    break;
                }
            }
        }
        if (remove) {
            events.remove(idx);
        }
        return remove;
    }

    @Override
    public String toString() {
        return "MemoryTransaction{" +
                "numberOfEvents=" + numberOfEvents +
                "} " + super.toString();
    }
}
