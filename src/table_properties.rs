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

use crate::ffi;
use std::{
    collections::BTreeMap,
    ffi::{c_char, c_int, c_void, CStr},
    ptr::addr_of_mut,
    slice,
};

pub trait TablePropertiesCollectorFactory {
    type Collector: TablePropertiesCollector;

    fn create(&mut self, ctx: TablePropertiesCollectorFactoryContext) -> Self::Collector;

    fn name(&self) -> &CStr;
}

unsafe extern "C" fn factory_destructor_callback<F>(raw_self: *mut c_void)
where
    F: TablePropertiesCollectorFactory,
{
    drop(Box::from_raw(raw_self as *mut F));
}

unsafe extern "C" fn factory_name_callback<F>(raw_self: *mut c_void) -> *const c_char
where
    F: TablePropertiesCollectorFactory,
{
    let self_ = &*(raw_self.cast_const() as *const F);
    self_.name().as_ptr()
}

unsafe extern "C" fn create_table_properties_collector_callback<F>(
    raw_self: *mut c_void,
    context: *const ffi::rocksdb_table_properties_collector_factory_context_t,
) -> *mut ffi::rocksdb_table_properties_collector_t
where
    F: TablePropertiesCollectorFactory,
{
    let self_ = &mut *(raw_self as *mut F);
    let context = TablePropertiesCollectorFactoryContext::from_raw(context);
    let collector = Box::new(self_.create(context));

    ffi::rocksdb_table_properties_collector_create(
        Box::into_raw(collector).cast::<c_void>(),
        Some(collector_destructor_callback::<F::Collector>),
        Some(collector_name_callback::<F::Collector>),
        Some(collector_add_user_key_callback::<F::Collector>),
        Some(collector_block_add_callback::<F::Collector>),
        Some(collector_finish_properties_callback::<F::Collector>),
        Some(collector_get_readable_properties_callback::<F::Collector>),
    )
}

pub(crate) unsafe fn create_table_properties_collector_factory<F>(
    factory: F,
) -> *mut ffi::rocksdb_table_properties_collector_factory_t
where
    F: TablePropertiesCollectorFactory,
{
    let factory = Box::new(factory);
    ffi::rocksdb_table_properties_collector_factory_create(
        Box::into_raw(factory).cast::<c_void>(),
        Some(factory_destructor_callback::<F>),
        Some(create_table_properties_collector_callback::<F>),
        Some(factory_name_callback::<F>),
    )
}

pub struct TablePropertiesCollectorFactoryContext {
    /// The level at creating the SST file (i.e, table), of which the properties are being collected.
    pub level_at_creation: i32,
}

impl TablePropertiesCollectorFactoryContext {
    unsafe fn from_raw(
        ctx: *const ffi::rocksdb_table_properties_collector_factory_context_t,
    ) -> Self {
        let level_at_creation =
            ffi::rocksdb_table_properties_collector_factory_context_level_at_creation(ctx);
        TablePropertiesCollectorFactoryContext { level_at_creation }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum EntryType {
    Put,
    Delete,
    SingleDelete,
    Merge,
    RangeDeletion,
    BlockIndex,
    DeleteWithTimestamp,
    WideColumnEntity,
    TimedPut,
    Other,
}

impl EntryType {
    fn from_raw(value: i32) -> Option<Self> {
        if value < 0 {
            return None;
        }
        match value as u32 {
            ffi::rocksdb_k_entry_put => Some(EntryType::Put),
            ffi::rocksdb_k_entry_delete => Some(EntryType::Delete),
            ffi::rocksdb_k_entry_single_delete => Some(EntryType::SingleDelete),
            ffi::rocksdb_k_entry_merge => Some(EntryType::Merge),
            ffi::rocksdb_k_entry_range_deletion => Some(EntryType::RangeDeletion),
            ffi::rocksdb_k_entry_block_index => Some(EntryType::BlockIndex),
            ffi::rocksdb_k_entry_delete_with_timestamp => Some(EntryType::DeleteWithTimestamp),
            ffi::rocksdb_k_entry_wide_column_entity => Some(EntryType::WideColumnEntity),
            ffi::rocksdb_k_entry_timed_put => Some(EntryType::TimedPut),
            ffi::rocksdb_k_entry_other => Some(EntryType::Other),
            _ => None,
        }
    }
}

pub trait TablePropertiesCollector {
    fn name(&self) -> &CStr;

    fn add_user_key(
        &mut self,
        key: &[u8],
        value: &[u8],
        entry_type: EntryType,
        seq: u64,
        file_size: u64,
    );

    fn block_add(
        &mut self,
        block_uncomp_bytes: u64,
        block_compressed_bytes_fast: u64,
        block_compressed_bytes_slow: u64,
    ) {
        let _ = (
            block_uncomp_bytes,
            block_compressed_bytes_fast,
            block_compressed_bytes_slow,
        );
    }

    fn finish_properties(&mut self) -> BTreeMap<Box<[u8]>, Box<[u8]>>;

    fn get_readable_properties(&mut self) -> BTreeMap<Box<[u8]>, Box<[u8]>> {
        BTreeMap::default()
    }
}

unsafe extern "C" fn collector_destructor_callback<C>(raw_self: *mut c_void)
where
    C: TablePropertiesCollector,
{
    drop(Box::from_raw(raw_self as *mut C));
}

unsafe extern "C" fn collector_name_callback<C>(raw_self: *mut c_void) -> *const c_char
where
    C: TablePropertiesCollector,
{
    let self_ = &*(raw_self.cast_const() as *const C);
    self_.name().as_ptr()
}

unsafe extern "C" fn collector_add_user_key_callback<C>(
    raw_self: *mut c_void,
    raw_key: *const c_char,
    key_len: usize,
    raw_value: *const c_char,
    value_len: usize,
    entry_type: c_int,
    seq: u64,
    file_size: u64,
) where
    C: TablePropertiesCollector,
{
    let self_ = &mut *(raw_self as *mut C);
    let key = slice::from_raw_parts(raw_key as *const u8, key_len);
    let value = slice::from_raw_parts(raw_value as *const u8, value_len);
    let entry_type = EntryType::from_raw(entry_type).unwrap();
    self_.add_user_key(key, value, entry_type, seq, file_size);
}

unsafe extern "C" fn collector_block_add_callback<C>(
    raw_self: *mut c_void,
    block_uncomp_bytes: u64,
    block_compressed_bytes_fast: u64,
    block_compressed_bytes_slow: u64,
) where
    C: TablePropertiesCollector,
{
    let self_ = &mut *(raw_self as *mut C);
    self_.block_add(
        block_uncomp_bytes,
        block_compressed_bytes_fast,
        block_compressed_bytes_slow,
    );
}

unsafe extern "C" fn collector_finish_properties_callback<C>(
    raw_self: *mut c_void,
    properties: *mut c_void,
    add_user_properties_callback: ffi::rocksdb_add_user_collected_properties,
) where
    C: TablePropertiesCollector,
{
    if let Some(callback) = add_user_properties_callback {
        let self_ = &mut *(raw_self as *mut C);
        for (key, value) in &self_.finish_properties() {
            callback(
                properties,
                key.as_ptr() as _,
                key.len(),
                value.as_ptr() as _,
                value.len(),
            );
        }
    }
}

unsafe extern "C" fn collector_get_readable_properties_callback<C>(
    raw_self: *mut c_void,
    properties: *mut c_void,
    add_user_properties_callback: ffi::rocksdb_add_user_collected_properties,
) where
    C: TablePropertiesCollector,
{
    if let Some(callback) = add_user_properties_callback {
        let self_ = &mut *(raw_self as *mut C);
        for (key, value) in &self_.get_readable_properties() {
            callback(
                properties,
                key.as_ptr() as _,
                key.len(),
                value.as_ptr() as _,
                value.len(),
            );
        }
    }
}

pub struct TablePropertiesCollection {
    pub tables: Vec<TableProperties>,
}

impl TablePropertiesCollection {
    pub(crate) unsafe fn from_raw(
        collection: *mut ffi::rocksdb_table_properties_collection_t,
    ) -> Self {
        let mut tables = vec![];
        loop {
            let properties = ffi::rocksdb_table_properties_collection_next(collection);
            if properties.is_null() {
                break;
            }
            tables.push(TableProperties::from_raw(properties));
        }
        TablePropertiesCollection { tables }
    }
}

pub struct TableProperties {
    inner: *mut ffi::rocksdb_table_properties_t,
}

impl TableProperties {
    unsafe fn from_raw(inner: *mut ffi::rocksdb_table_properties_t) -> Self {
        TableProperties { inner }
    }

    pub fn name(&self) -> &CStr {
        unsafe {
            let name = ffi::rocksdb_table_properties_table_name(self.inner);
            CStr::from_ptr(name)
        }
    }

    pub fn user_collected_properties(&self) -> BTreeMap<Box<[u8]>, Box<[u8]>> {
        unsafe {
            let mut map = BTreeMap::new();
            ffi::rocksdb_table_properties_user_collected(
                self.inner,
                addr_of_mut!(map) as _,
                Some(table_property_reader),
            );
            map
        }
    }

    pub fn readable_properties(&self) -> BTreeMap<Box<[u8]>, Box<[u8]>> {
        unsafe {
            let mut map = BTreeMap::new();
            ffi::rocksdb_table_properties_readable(
                self.inner,
                addr_of_mut!(map) as _,
                Some(table_property_reader),
            );
            map
        }
    }
}

impl Drop for TableProperties {
    fn drop(&mut self) {
        unsafe { ffi::rocksdb_table_properties_destroy(self.inner) }
    }
}

unsafe extern "C" fn table_property_reader(
    state: *mut c_void,
    key_data: *const c_char,
    key_len: usize,
    value_data: *const c_char,
    value_len: usize,
) {
    let map = &mut *(state as *mut BTreeMap<Box<[u8]>, Box<[u8]>>);
    let key = slice::from_raw_parts(key_data as *const u8, key_len);
    let value = slice::from_raw_parts(value_data as *const u8, value_len);

    map.insert(key.to_vec().into(), value.to_vec().into());
}
