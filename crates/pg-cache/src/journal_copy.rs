//! Experimental binary COPY source. Runs exclusively on the PostgreSQL thread.
//! PostgreSQL's COPY executor owns constraints, indexes, WAL and bulk insertion.
use pgrx::{pg_guard, pg_sys};
use std::{cell::RefCell, ptr};
thread_local! {
    static INPUT: RefCell<Option<(Vec<u8>, usize)>> = const { RefCell::new(None) };
}
struct InputGuard;
impl Drop for InputGuard {
    fn drop(&mut self) {
        INPUT.with(|input| *input.borrow_mut() = None);
    }
}
#[pg_guard]
unsafe extern "C-unwind" fn read(out: *mut std::ffi::c_void, _min: i32, max: i32) -> i32 {
    assert!(max >= 0);
    INPUT.with(|input| {
        let mut source = input.borrow_mut();
        let (bytes, position) = source.as_mut().expect("COPY source");
        let count = (max as usize).min(bytes.len() - *position);
        // COPY supplies a writable max-byte buffer; source remains owned here.
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr().add(*position), out.cast(), count);
        }
        *position += count;
        count as i32
    })
}
fn field(output: &mut Vec<u8>, bytes: &[u8]) {
    output.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
    output.extend_from_slice(bytes);
}
pub(crate) fn insert(slot: usize, sequence: i64, previous: i64, payload: &[u8]) {
    assert!(slot < 2);
    let chunks = payload.chunks(6144);
    let parts = chunks.len() as i32;
    let mut input = Vec::with_capacity(payload.len() + parts as usize * 46 + 21);
    input.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    input.extend_from_slice(&0i32.to_be_bytes());
    input.extend_from_slice(&0i32.to_be_bytes());
    for (part, chunk) in chunks.enumerate() {
        input.extend_from_slice(&5i16.to_be_bytes());
        field(&mut input, &sequence.to_be_bytes());
        field(&mut input, &previous.to_be_bytes());
        field(&mut input, &(part as i32).to_be_bytes());
        field(&mut input, &parts.to_be_bytes());
        field(&mut input, chunk);
    }
    input.extend_from_slice(&(-1i16).to_be_bytes());
    INPUT.with(|source| {
        let mut source = source.borrow_mut();
        assert!(source.is_none(), "recursive journal COPY");
        *source = Some((input, 0));
    });
    let _input_guard = InputGuard;
    // All pointers are PostgreSQL-owned, used in the active transaction on its
    // main thread. Resource owners release relation locks on PostgreSQL ERROR;
    // pgrx unwinding drops the Rust source guard. No file or socket is opened.
    unsafe {
        let namespace = pg_sys::get_namespace_oid(c"cache_journal".as_ptr(), false);
        let name = if slot == 0 { c"segment0" } else { c"segment1" };
        let oid = pg_sys::get_relname_relid(name.as_ptr(), namespace);
        let relation = pg_sys::table_open(oid, pg_sys::RowExclusiveLock as i32);
        let class = &*(*relation).rd_rel;
        assert!(
            class.relkind == b'r' as std::ffi::c_char
                && class.relpersistence == b'p' as std::ffi::c_char
                && class.relam == pg_sys::HEAP_TABLE_AM_OID
                && !class.relrowsecurity
                && !class.relforcerowsecurity
                && !class.relispartition,
            "unsupported journal relation"
        );
        let format = pg_sys::makeDefElem(
            pg_sys::pstrdup(c"format".as_ptr()),
            pg_sys::makeString(pg_sys::pstrdup(c"binary".as_ptr())).cast(),
            -1,
        );
        let options = pg_sys::list_make1_impl(
            pg_sys::NodeTag::T_List,
            pg_sys::ListCell {
                ptr_value: format.cast(),
            },
        );
        let parse = pg_sys::make_parsestate(ptr::null_mut());
        let namespace_item = pg_sys::addRangeTableEntryForRelation(
            parse,
            relation,
            pg_sys::RowExclusiveLock as i32,
            ptr::null_mut(),
            false,
            false,
        );
        (*(*namespace_item).p_perminfo).requiredPerms = pg_sys::ACL_INSERT.into();
        pg_sys::ExecCheckPermissions((*parse).p_rtable, (*parse).p_rteperminfos, true);
        let copy = pg_sys::BeginCopyFrom(
            parse,
            relation,
            ptr::null_mut(),
            ptr::null(),
            false,
            Some(read),
            ptr::null_mut(),
            options,
        );
        let rows = pg_sys::CopyFrom(copy);
        pg_sys::EndCopyFrom(copy);
        pg_sys::free_parsestate(parse);
        pg_sys::table_close(relation, pg_sys::NoLock as i32);
        assert_eq!(rows, parts as u64, "all journal parts inserted");
        pg_sys::CommandCounterIncrement();
    }
}
