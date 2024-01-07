# wal-dbstore

This is a simple implementation of Write Ahead log for Key value DB and it will be logging all
the transactions done by the Key Value DB. This application is not thread safe. It is assumed that it will be only handling inputs in a sequential manner. 
When the WAL performs log compaction if any other operation is performed on the log 
then it will be throwing a Runtime Exception.