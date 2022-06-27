use pgutils::migrate::Migrator;

#[test]
fn test_simple_load() {
    let m: Migrator = pgutils::migrate::embed!("tests/stubs/simple");

    assert_eq!(1, m.migrations.len());
    assert_eq!("simple_migration", m.migrations[0].name);
    assert_eq!("SELECT 1 AS one;", m.migrations[0].sql);
    assert_eq!(1614877844, m.migrations[0].version);

    assert_eq!(
        "15eeddc5378a3c6645059e83c780643fb121432deedfb19a2dec16154e2f93ca",
        m.migrations[0].checksum
    );
}
