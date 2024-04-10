use storage::Storage;

fn main() {
    let mut storage = Storage::open("test.kiv").expect("failed");

    storage
        .write_data_entry("test key", "test value hello")
        .unwrap();
    assert_eq!(
        storage.get_data_entry(&"test key".to_string()).unwrap(),
        Some("test value hello".to_string())
    );
    storage
        .write_data_entry("test2", "test value hello2")
        .unwrap();
    assert_eq!(
        storage.get_data_entry(&"test2".to_string()).unwrap(),
        Some("test value hello2".to_string())
    );
    storage.delete_data_entry(&"test key".to_string()).unwrap();
    assert_eq!(
        storage.get_data_entry(&"test key".to_string()).unwrap(),
        None
    );
    assert_eq!(
        storage.get_data_entry(&"test2".to_string()).unwrap(),
        Some("test value hello2".to_string())
    );
    storage
        .update_data_entry(&"test2".to_string(), &"updated value".to_string())
        .unwrap();

    assert_eq!(
        storage.get_data_entry(&"test2".to_string()).unwrap(),
        Some("updated value".to_string())
    );
}
