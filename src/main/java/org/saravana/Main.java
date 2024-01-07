package org.saravana;

import org.saravana.wal.impl.WriteAheadLogFile;

public class Main {
    public static void main(String[] args) {
        WriteAheadLogFile writeAheadLogFile = new WriteAheadLogFile("filename.log", "checkpointFile.log");
        writeAheadLogFile.write("123", "123");
        writeAheadLogFile.write("456", "123");
        writeAheadLogFile.write("789", "123");
        writeAheadLogFile.write("234", "123");
        writeAheadLogFile.update("123", "456");
        writeAheadLogFile.delete("123");
        writeAheadLogFile.flush();
        writeAheadLogFile.createCheckPoint();

    }
}