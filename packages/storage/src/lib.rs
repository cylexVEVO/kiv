use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
};

use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, Bytes, BytesMut};

const MAGIC_BYTES: [u8; 6] = [0, 104, 105, 107, 105, 118];
const CURRENT_VERSION: u16 = 0;
const DATA_ENTRY_TYPE: u8 = 0;

pub struct Storage {
    file: File,
}

struct DataEntry {
    key: String,
    value: String,
}

impl DataEntry {
    fn from(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::new();

        bytes.put_u8(DATA_ENTRY_TYPE);
        bytes.put_u16(self.key.len() as u16);
        bytes.put(self.key.as_bytes());
        bytes.put_u32(self.value.len() as u32);
        bytes.put(self.value.as_bytes());

        return Bytes::from(bytes);
    }
}

impl Into<Bytes> for DataEntry {
    fn into(self) -> Bytes {
        return self.to_bytes();
    }
}

impl Storage {
    fn initialize_file(file: &mut File) {
        let mut bytes = BytesMut::new();

        // write file identifier
        bytes.extend_from_slice(&MAGIC_BYTES);
        // write version number
        bytes.put_u16(CURRENT_VERSION);

        file.write_all(&bytes).expect("failed to write to file");
    }

    pub fn open(path: impl Into<String>) -> std::io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path.into())?;

        // see if file needs to be initialized
        let mut buf = [0];
        if file.read(&mut buf)? == 0 {
            Self::initialize_file(&mut file);

            return Ok(Self { file });
        }

        // validate file
        let check_mb = [0u8; 6];
        if file.read(&mut buf)? != 6 || check_mb != MAGIC_BYTES {
            // reinitialize file
            file.set_len(0)?;
            Self::initialize_file(&mut file);
        }

        return Ok(Self { file });
    }

    pub fn write_data_entry(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> std::io::Result<()> {
        let entry = DataEntry::from(key, value);

        self.file.seek(std::io::SeekFrom::End(0))?;
        self.file.write_all(&entry.to_bytes())?;

        return Ok(());
    }

    pub fn get_data_entry(&mut self, search_key: &String) -> std::io::Result<Option<String>> {
        let entry_offset = if let Some(offset) = self.get_data_entry_offset(search_key)? {
            offset
        } else {
            return Ok(None);
        };
        // adding 1 to offset to skip entry type
        self.file.seek(std::io::SeekFrom::Start(entry_offset + 1))?;

        // read key length
        let mut key_len = [0, 0];
        if self.file.read(&mut key_len)? != 2 {
            panic!("unknown error");
        }
        let key_len = BigEndian::read_u16(&key_len) as usize;

        // skip key, we don't need it
        self.file.seek(std::io::SeekFrom::Current(key_len as i64))?;

        // read value length
        let mut value_len = [0, 0, 0, 0];
        if self.file.read(&mut value_len)? != 4 {
            panic!("unknown error");
        }
        let value_len = BigEndian::read_u32(&value_len) as usize;

        // read value
        let mut value = vec![0u8; value_len];
        if self.file.read(&mut value)? != value_len {
            panic!("unknown error");
        }
        // TODO: proper error handling
        let value = String::from_utf8(value).expect("failure");

        return Ok(Some(value));
    }

    fn get_data_entry_offset(&mut self, search_key: &String) -> std::io::Result<Option<u64>> {
        // skip file header
        self.file.seek(std::io::SeekFrom::Start(8))?;
        let mut buf = [0];
        loop {
            match self.file.read(&mut buf) {
                Ok(0) => {
                    return Ok(None);
                }
                Ok(_) => {
                    match &buf[0] {
                        // data entry
                        &DATA_ENTRY_TYPE => {
                            // store current offset incase it's the one we need
                            // subtracting 1 to make up for entry type
                            let offset =
                                (self.file.seek(std::io::SeekFrom::Current(0))? - 1) as u64;

                            // read key length
                            let mut key_len = [0, 0];
                            if self.file.read(&mut key_len)? != 2 {
                                panic!("unknown error");
                            }
                            let key_len = BigEndian::read_u16(&key_len) as usize;

                            // read key
                            let mut key = vec![0u8; key_len];
                            if self.file.read(&mut key)? != key_len {
                                panic!("unknown error");
                            }
                            // TODO: proper error handling
                            let key = String::from_utf8(key).expect("failure");

                            // read value length
                            let mut value_len = [0, 0, 0, 0];
                            if self.file.read(&mut value_len)? != 4 {
                                panic!("unknown error");
                            }
                            let value_len = BigEndian::read_u32(&value_len) as usize;

                            // if read key matches search key, return offset
                            if &key == search_key {
                                return Ok(Some(offset));
                            }

                            // else, skip the value
                            self.file
                                .seek(std::io::SeekFrom::Current(value_len as i64))?;
                        }
                        _ => panic!("unknown data entry"),
                    }
                }
                e => panic!("{:#?}", e),
            }
        }
    }

    fn get_data_entry_length(&mut self, search_key: &String) -> std::io::Result<Option<u64>> {
        let entry_offset = if let Some(offset) = self.get_data_entry_offset(search_key)? {
            offset
        } else {
            return Ok(None);
        };
        // adding 1 to offset to skip entry type
        self.file.seek(std::io::SeekFrom::Start(entry_offset + 1))?;

        // read key length
        let mut key_len = [0, 0];
        if self.file.read(&mut key_len)? != 2 {
            panic!("unknown error");
        }
        let key_len = BigEndian::read_u16(&key_len) as usize;

        // skip key, we don't need it
        self.file.seek(std::io::SeekFrom::Current(key_len as i64))?;

        // read value length
        let mut value_len = [0, 0, 0, 0];
        if self.file.read(&mut value_len)? != 4 {
            panic!("unknown error");
        }
        let value_len = BigEndian::read_u32(&value_len) as usize;

        // entry type, key length length, key length, value length length, value length
        let entry_length = 1 + 2 + key_len + 4 + value_len;

        return Ok(Some(entry_length as u64));
    }

    pub fn delete_data_entry(&mut self, search_key: &String) -> std::io::Result<()> {
        let entry_offset = if let Some(offset) = self.get_data_entry_offset(search_key)? {
            offset
        } else {
            return Ok(());
        };

        let entry_length = if let Some(length) = self.get_data_entry_length(search_key)? {
            length
        } else {
            return Ok(());
        };

        self.file
            .seek(std::io::SeekFrom::Start(entry_offset + entry_length))?;
        // read the data we are shifting
        let mut data_to_shift = vec![];
        self.file.read_to_end(&mut data_to_shift)?;
        // truncate file
        self.file.set_len(entry_offset)?;
        // seek back to correct location for data to shift
        self.file.seek(std::io::SeekFrom::Start(entry_offset))?;
        // write back the data we needed to shift
        self.file.write_all(&data_to_shift)?;

        return Ok(());
    }

    pub fn update_data_entry(
        &mut self,
        search_key: &String,
        new_value: &String,
    ) -> std::io::Result<()> {
        let entry_offset = if let Some(offset) = self.get_data_entry_offset(search_key)? {
            offset
        } else {
            return Ok(());
        };

        let entry_length = if let Some(length) = self.get_data_entry_length(search_key)? {
            length
        } else {
            return Ok(());
        };

        let new_entry = DataEntry::from(search_key.clone(), new_value.clone());

        self.file
            .seek(std::io::SeekFrom::Start(entry_offset + entry_length))?;
        // read the data we are shifting
        let mut data_to_shift = vec![];
        self.file.read_to_end(&mut data_to_shift)?;
        // truncate file
        self.file.set_len(entry_offset)?;
        // seek back to correct location for new entry
        self.file.seek(std::io::SeekFrom::End(0))?;
        // write new record
        self.file.write_all(&new_entry.to_bytes())?;
        // seek back to correct location for data to shift
        self.file.seek(std::io::SeekFrom::End(0))?;
        // write back the data we needed to shift
        self.file.write_all(&data_to_shift)?;

        return Ok(());
    }
}
