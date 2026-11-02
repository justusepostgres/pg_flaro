//! Private-table fast path. PostgreSQL owns visibility, WAL, HOT, and index updates.
use pgrx::{pg_sys, FromDatum, IntoDatum, Spi};
use std::ffi::CStr;

// CatalogTupleUpdateWithInfo omits general executor constraints/triggers. Accept
// exactly our plain heap and one-column hash index; otherwise SQL
// handles every row. The caller holds a lock excluding DML and schema changes.
unsafe fn compatible(relation: pg_sys::Relation, indexes: pg_sys::CatalogIndexState) -> bool {
    let class = &*(*relation).rd_rel;
    let desc = &*(*relation).rd_att;
    if class.relam != pg_sys::HEAP_TABLE_AM_OID
        || class.relkind != b'r' as std::ffi::c_char
        || class.relpersistence != b'p' as std::ffi::c_char
        || !(*relation).trigdesc.is_null()
        || class.relhasrules
        || class.relrowsecurity
        || class.relforcerowsecurity
        || class.relhassubclass
        || class.relispartition
        || class.relchecks != 0
        || desc.natts != 4
        || (*indexes).ri_NumIndices != 1
    {
        return false;
    }
    for (attr, (name, oid, not_null)) in desc.attrs.as_slice(4).iter().zip([
        (c"key", pg_sys::BYTEAOID, true),
        (c"value", pg_sys::BYTEAOID, true),
        (c"expires_at_ms", pg_sys::INT8OID, false),
        (c"data_type", pg_sys::INT2OID, true),
    ]) {
        if attr.atttypid != oid
            || attr.attnotnull != not_null
            || attr.attisdropped
            || attr.attgenerated != 0
            || attr.attidentity != 0
            || attr.attinhcount != 0
            || CStr::from_ptr(attr.attname.data.as_ptr()) != name
        {
            return false;
        }
    }
    let index = *(*indexes).ri_IndexRelationDescs;
    let info = &**(*indexes).ri_IndexRelationInfo;
    let definition = &*(*index).rd_index;
    (*(*index).rd_rel).relam == pg_sys::HASH_AM_OID
        && !definition.indisprimary
        && !definition.indisunique
        && definition.indimmediate
        && definition.indisvalid
        && definition.indisready
        && definition.indislive
        && info.ii_NumIndexAttrs == 1
        && info.ii_NumIndexKeyAttrs == 1
        && info.ii_IndexAttrNumbers[0] == 1
        && info.ii_ReadyForInserts
        && info.ii_Expressions.is_null()
        && info.ii_Predicate.is_null()
        && info.ii_ExclusionOps.is_null()
}

pub fn update_existing(
    keys: &[&[u8]],
    values: &[&[u8]],
    expirations: &[Option<i64>],
    types: &[i16],
    locations: &[Option<pg_sys::ItemPointerData>],
) -> Vec<(usize, pg_sys::ItemPointerData)> {
    assert_eq!(keys.len(), values.len());
    assert_eq!(keys.len(), types.len());
    assert_eq!(keys.len(), expirations.len());
    assert_eq!(keys.len(), locations.len());
    Spi::run("LOCK TABLE cache_store.entries IN SHARE ROW EXCLUSIVE MODE")
        .expect("lock cache storage");
    // SAFETY: only called on the PG worker thread in an active transaction.
    // Relation and index descriptors remain open for all pointer accesses. The
    // table lock excludes concurrent writes, truncation, DDL and vacuum truncation.
    // Buffer pins protect fetched tuple data; heap_fetch checks stale offsets,
    // while we check block bounds before any buffer access. PostgreSQL resource
    // owners release locks/buffers on ERROR. Normal paths explicitly release them.
    unsafe {
        let namespace = pg_sys::get_namespace_oid(c"cache_store".as_ptr(), false);
        let oid = pg_sys::get_relname_relid(c"entries".as_ptr(), namespace);
        let relation = pg_sys::table_open(oid, pg_sys::ShareRowExclusiveLock as i32);
        let indexes = pg_sys::CatalogOpenIndexes(relation);
        let mut changed = Vec::new();
        if compatible(relation, indexes) {
            let desc = (*relation).rd_att;
            let snapshot = pg_sys::GetLatestSnapshot();
            let blocks =
                pg_sys::RelationGetNumberOfBlocksInFork(relation, pg_sys::ForkNumber::MAIN_FORKNUM);
            for (index, location) in locations.iter().enumerate() {
                let Some(tid) = location else {
                    continue;
                };
                let block = pgrx::itemptr::item_pointer_get_block_number(tid);
                if block >= blocks {
                    continue;
                }
                let prune_buffer = pg_sys::ReadBuffer(relation, block);
                pg_sys::heap_page_prune_opt(relation, prune_buffer);
                pg_sys::ReleaseBuffer(prune_buffer);
                let mut old = pg_sys::HeapTupleData {
                    t_self: *tid,
                    ..Default::default()
                };
                let mut buffer = 0;
                if !pg_sys::heap_fetch(relation, snapshot, &mut old, &mut buffer, false) {
                    continue;
                }
                let mut is_null = false;
                let datum = pg_sys::heap_getattr(&mut old, 1, desc, &mut is_null);
                let matches = Vec::<u8>::from_datum(datum, is_null)
                    .is_some_and(|stored| stored == keys[index]);
                pg_sys::ReleaseBuffer(buffer);
                if !matches {
                    continue;
                }
                let key = keys[index].into_datum().unwrap();
                let value = values[index].into_datum().unwrap();
                let mut datums = [
                    key,
                    value,
                    expirations[index]
                        .into_datum()
                        .unwrap_or(pg_sys::Datum::from(0usize)),
                    types[index].into_datum().unwrap(),
                ];
                let mut nulls = [false, false, expirations[index].is_none(), false];
                let new = pg_sys::heap_form_tuple(desc, datums.as_mut_ptr(), nulls.as_mut_ptr());
                pg_sys::CatalogTupleUpdateWithInfo(relation, &mut old.t_self, new, indexes);
                changed.push((index, (*new).t_self));
                pg_sys::heap_freetuple(new);
                pg_sys::pfree(key.cast_mut_ptr());
                pg_sys::pfree(value.cast_mut_ptr());
            }
            // Subsequent SQL fallback/checkpoint operations must see these writes.
            pg_sys::CommandCounterIncrement();
        }
        pg_sys::CatalogCloseIndexes(indexes);
        pg_sys::table_close(relation, pg_sys::NoLock as i32);
        changed
    }
}
