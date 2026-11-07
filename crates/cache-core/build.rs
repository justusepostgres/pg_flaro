fn main() {
    println!("cargo:rerun-if-changed=src/long_double.c");
    cc::Build::new()
        .file("src/long_double.c")
        .flag_if_supported("-std=c11")
        .compile("pgcache_long_double");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux") {
        println!("cargo:rustc-link-lib=m");
    }
}
