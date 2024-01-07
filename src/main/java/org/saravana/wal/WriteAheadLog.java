package org.saravana.wal;

public interface WriteAheadLog {
    void write(String key, String value);
    void update(String key, String value);
    void delete(String key);
    void flush();
    void recover();

    void createCheckPoint();
}
