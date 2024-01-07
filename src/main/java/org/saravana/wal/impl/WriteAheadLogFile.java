package org.saravana.wal.impl;

import org.saravana.wal.WriteAheadLog;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class WriteAheadLogFile implements WriteAheadLog {
    private File checkpointFile;
    private File logFile;

    @Override
    public void write(String key, String value) {
        try (FileWriter writer = new FileWriter(logFile, true)) {
            writer.write("WRITE " + key + " " + value + "\n");
        } catch (IOException e) {
            // Handle the exception
        }
    }

    @Override
    public void update(String key, String value) {
        try (FileWriter writer = new FileWriter(logFile, true)) {
            writer.write("UPDATE " + key + " " + value + "\n");
        } catch (IOException e) {
            // Handle the exception
        }
    }


    @Override
    public void delete(String key) {
        try (FileWriter writer = new FileWriter(logFile, true)) {
            writer.write("DELETE " + key +  "\n");
        } catch (IOException e) {
            // Handle the exception
        }
    }

    @Override
    public void flush() {
        try (FileWriter writer = new FileWriter(logFile, true)) {
            writer.flush();
        } catch (IOException e) {
            // Handle the exception
        }
    }

    @Override
    public void recover() {
// Implement recovery logic here
        // Read the entries from the log file and apply them to restore the database state
        try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Apply the operations recorded in the log to restore the database state
                // For example, if a line contains "WRITE key value", you would add "key" with "value" to the database
                // If a line contains "UPDATE key value", you would update "key" with "value" in the database
                // If a line contains "DELETE key", you would mark "key" as deleted in the database
                // This is just a simplified example, and the actual recovery logic would depend on the database implementation
            }
        } catch (IOException e) {
            // Handle the exception
        }
    }

    public WriteAheadLogFile(String logFilePath, String checkpointFilePath) {
        this.logFile = new File(logFilePath);
        this.checkpointFile = new File(checkpointFilePath);
    }

    // Other methods such as write, update, delete, flush, and recover would be implemented here

    public void createCheckpoint() {
        // Implement checkpoint logic to create a checkpoint file
        // ...
        compactLogSimple();
    }

    private void compactLogSimple() {
        Map<String, String> compactedLog = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ");
                String operation = parts[0];
                String key = parts[1];
                String value = parts[2];
                // Consolidate redundant updates for the same key
                if (operation.equals("WRITE") || operation.equals("UPDATE")) {
                    compactedLog.put(key, value);
                } else if (operation.equals("DELETE")) {
                    compactedLog.remove(key);
                }
            }
        } catch (IOException e) {
            // Handle the exception
        }

        // Write the compacted log to a new log file
        File compactedLogFile = new File("compacted_log.txt");
        try (FileWriter writer = new FileWriter(compactedLogFile)) {
            for (Map.Entry<String, String> entry : compactedLog.entrySet()) {
                writer.write("WRITE " + entry.getKey() + " " + entry.getValue() + "\n");
            }
        } catch (IOException e) {
            // Handle the exception
        }

        // Replace the original log file with the compacted log file
        if (compactedLogFile.exists()) {
            if (logFile.delete()) {
                if (!compactedLogFile.renameTo(logFile)) {
                    // Handle the rename failure
                }
            } else {
                // Handle the delete failure
            }
        }
    }
}
}