namespace py chord

service ChordNode {
    KeyValueResult lookup(1: string key),
    Node find_successor(1: i32 key_id),
    KeyValueResult put(1: string key, 2: string value, 3: bool backup),
    void join(1: Node node),
    void notify(1: Node node),
    Node get_predecessor(),
    Node get_successor(),
    string heart_beat(),
    void backup(1: string backup_kv_store_key, 2: string key, 3: string value),
    void replay(1: KVStoreType kv_store_type),
}

enum KVStatus {
    VALID, NOT_FOUND
}

enum KVStoreType {
    BASE, PRE_BACKUP, PRE_PRE_BACKUP
}

struct KeyValueResult {
    1: string key,
    2: string value,
    3: i32 node_id,
    4: KVStatus status,
}

struct Node {
    1: i32 node_id,
    2: string address,
    3: i32 port,
    4: bool valid,
}

