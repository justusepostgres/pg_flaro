//! Planner scope for bounded exact-key persistence batches.
use pgrx::Spi;

pub(crate) fn run<T>(operation: impl FnOnce() -> T) -> T {
    // Fresh tables have no column statistics, and a nonunique hash index cannot
    // tell the planner that our serialized writes maintain one row per key.
    // Hash/merge joins can therefore scan the growing table for every batch.
    // These small batches need indexed nested loops, without per-batch JIT.
    let settings = ["enable_hashjoin", "enable_mergejoin", "jit"];
    let previous = settings.map(|name| {
        let enabled = Spi::get_one::<bool>(&format!("SELECT current_setting('{name}')::boolean"))
            .expect("read persistence planner setting")
            .expect("planner setting exists");
        Spi::run(&format!("SET LOCAL {name}=off")).expect("set persistence planner setting");
        enabled
    });
    let result = operation();
    // Preserve callers' settings when invoked through the SQL validation API.
    // An error aborts the transaction, which also rolls back SET LOCAL.
    for (name, enabled) in settings.into_iter().zip(previous) {
        let value = if enabled { "on" } else { "off" };
        Spi::run(&format!("SET LOCAL {name}={value}")).expect("restore planner setting");
    }
    result
}
