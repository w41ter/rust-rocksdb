// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod util;

use std::{
    collections::BTreeMap,
    ffi::{CStr, CString},
};

use rocksdb::{
    table_properties::{
        EntryType, TablePropertiesCollector, TablePropertiesCollectorFactory,
        TablePropertiesCollectorFactoryContext,
    },
    Options, DB,
};
use util::DBPath;

struct TablePropertiesCollectorImpl {
    name: CString,
    num_keys: usize,
    total_bytes: usize,
}

impl TablePropertiesCollector for TablePropertiesCollectorImpl {
    fn name(&self) -> &CStr {
        &self.name
    }

    fn add_user_key(
        &mut self,
        key: &[u8],
        value: &[u8],
        entry_type: EntryType,
        _seq: u64,
        _file_size: u64,
    ) {
        if entry_type == EntryType::Put {
            self.num_keys += 1;
            self.total_bytes += key.len() + value.len()
        }
    }

    fn block_add(
        &mut self,
        _block_uncomp_bytes: u64,
        _block_compressed_bytes_fast: u64,
        _block_compressed_bytes_slow: u64,
    ) {
    }

    fn finish_properties(&mut self) -> BTreeMap<Box<[u8]>, Box<[u8]>> {
        let mut map = BTreeMap::new();
        map.insert(
            "num-keys".as_bytes().into(),
            self.num_keys.to_string().into_bytes().into(),
        );
        map.insert(
            "total-bytes".as_bytes().into(),
            self.total_bytes.to_string().into_bytes().into(),
        );
        map
    }

    fn get_readable_properties(&mut self) -> BTreeMap<Box<[u8]>, Box<[u8]>> {
        BTreeMap::default()
    }
}

struct TablePropertiesCollectorFactoryImpl {
    name: CString,
}

impl TablePropertiesCollectorFactory for TablePropertiesCollectorFactoryImpl {
    type Collector = TablePropertiesCollectorImpl;

    fn create(&mut self, _ctx: TablePropertiesCollectorFactoryContext) -> Self::Collector {
        TablePropertiesCollectorImpl {
            name: CString::new("table-properties-collector").unwrap(),
            num_keys: 0,
            total_bytes: 0,
        }
    }

    fn name(&self) -> &CStr {
        &self.name
    }
}

#[test]
fn test_table_properties_collector() {
    let path = DBPath::new("_table_properties_collector");
    {
        let factory = TablePropertiesCollectorFactoryImpl {
            name: CString::new("table-properties-collector-factory").unwrap(),
        };

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.add_table_properties_collector_factory(factory);
        let mut db = DB::open(&opts, &path).unwrap();
        db.create_cf("cf", &opts).unwrap();
        let cf = db.cf_handle("cf").unwrap();
        db.put_cf(&cf, "k1", "a").unwrap();
        db.flush_cf(&cf).unwrap();
        let collection = db.get_properties_of_all_range(&cf).unwrap();
        for table in collection.tables {
            let table_properties = table.user_collected_properties();
            let num_keys = table_properties
                .get(b"num-keys".as_slice())
                .expect("num-keys must exists");
            let num_keys =
                std::str::from_utf8(num_keys).expect("the value of num-keys is utf8 encoded");
            assert_eq!(num_keys, "1");
        }
    }
}
