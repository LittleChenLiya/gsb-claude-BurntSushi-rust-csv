use csv::ByteRecord;
use csv::migrate::{
    FieldOrigin, MigrateError, MigrationSpec, PlanStep,
};

fn ids(cols: &[&str]) -> Vec<Vec<u8>> {
    cols.iter().map(|c| c.as_bytes().to_vec()).collect()
}

fn brec(fields: &[&[u8]]) -> ByteRecord {
    let mut r = ByteRecord::new();
    for f in fields {
        r.push_field(f);
    }
    r
}

#[test]
fn migrate_reorder_with_default() {
    // Old columns a,b migrate to new columns b,c,a; c defaults to x.
    let spec = MigrationSpec::new(ids(&["a", "b"]), ids(&["b", "c", "a"]))
        .default(b"c", b"x");
    let plan = spec.plan().unwrap();
    assert_eq!(
        plan.steps(),
        &[
            PlanStep::Copy {
                old_index: 1,
                old_id: b"b".to_vec(),
                new_id: b"b".to_vec(),
            },
            PlanStep::Default {
                new_id: b"c".to_vec(),
                bytes: b"x".to_vec(),
            },
            PlanStep::Copy {
                old_index: 0,
                old_id: b"a".to_vec(),
                new_id: b"a".to_vec(),
            },
        ]
    );
    assert_eq!(
        plan.describe(),
        "copy 1:b -> b\ndefault c = \"x\"\ncopy 0:a -> a\n"
    );
    // Record 1,2 becomes 2,x,1.
    let out = plan.execute(&brec(&[b"1", b"2"])).unwrap();
    assert_eq!(out.record(), &brec(&[b"2", b"x", b"1"]));
    assert_eq!(
        out.origins(),
        &[
            FieldOrigin::Copied { old_index: 1, old_id: b"b".to_vec() },
            FieldOrigin::Default { new_id: b"c".to_vec() },
            FieldOrigin::Copied { old_index: 0, old_id: b"a".to_vec() },
        ]
    );
}

#[test]
fn migrate_explicit_rename_not_display_name() {
    // IDs never match by display name: a rename must be declared.
    let spec = MigrationSpec::new(ids(&["a", "b"]), ids(&["b", "c"]))
        .rename(b"a", b"c");
    let plan = spec.plan().unwrap();
    let out = plan.execute(&brec(&[b"1", b"2"])).unwrap();
    assert_eq!(out.record(), &brec(&[b"2", b"1"]));
}

#[test]
fn migrate_duplicate_target_id_rejected() {
    let spec = MigrationSpec::new(ids(&["a", "b"]), ids(&["b", "b"]));
    assert_eq!(
        spec.plan(),
        Err(MigrateError::DuplicateTargetId(b"b".to_vec()))
    );
}

#[test]
fn migrate_duplicate_source_id_rejected() {
    let spec = MigrationSpec::new(ids(&["a", "a"]), ids(&["a"]));
    assert_eq!(
        spec.plan(),
        Err(MigrateError::DuplicateSourceId(b"a".to_vec()))
    );
}

#[test]
fn migrate_missing_required_default_rejected() {
    // New column c has no source and no default: reject.
    let spec = MigrationSpec::new(ids(&["a", "b"]), ids(&["b", "c", "a"]));
    assert_eq!(
        spec.plan(),
        Err(MigrateError::MissingDefault(b"c".to_vec()))
    );
}

#[test]
fn migrate_deletion_requires_explicit_authorization() {
    // Old column b is dropped by the new layout.
    let spec = MigrationSpec::new(ids(&["a", "b"]), ids(&["a"]));
    assert_eq!(
        spec.plan(),
        Err(MigrateError::DeletionNotAuthorized(vec![b"b".to_vec()]))
    );
    // With the policy explicitly allowing deletions, planning succeeds.
    let plan = spec.allow_deletions(true).plan().unwrap();
    let out = plan.execute(&brec(&[b"1", b"2"])).unwrap();
    assert_eq!(out.record(), &brec(&[b"1"]));
}

#[test]
fn migrate_width_mismatch_rejected_without_partial_record() {
    let spec = MigrationSpec::new(ids(&["a", "b"]), ids(&["b", "c", "a"]))
        .default(b"c", b"x");
    let plan = spec.plan().unwrap();
    // Too few fields: no partial new record is returned.
    assert_eq!(
        plan.execute(&brec(&[b"1"])),
        Err(MigrateError::WidthMismatch { expected: 2, got: 1 })
    );
    // Too many fields: also rejected.
    assert_eq!(
        plan.execute(&brec(&[b"1", b"2", b"3"])),
        Err(MigrateError::WidthMismatch { expected: 2, got: 3 })
    );
}

#[test]
fn migrate_empty_field_and_default_have_distinct_origins() {
    let spec = MigrationSpec::new(ids(&["a", "b"]), ids(&["b", "c", "a"]))
        .default(b"c", b"");
    let plan = spec.plan().unwrap();
    // The old record has an empty b field; c gets an empty default.
    let out = plan.execute(&brec(&[b"1", b""])).unwrap();
    assert_eq!(out.record(), &brec(&[b"", b"", b"1"]));
    // Both output fields are empty bytes, but their origins differ.
    assert_eq!(
        out.origins(),
        &[
            FieldOrigin::Copied { old_index: 1, old_id: b"b".to_vec() },
            FieldOrigin::Default { new_id: b"c".to_vec() },
            FieldOrigin::Copied { old_index: 0, old_id: b"a".to_vec() },
        ]
    );
}

#[test]
fn migrate_conflicting_sources_rejected() {
    // Rename b -> a collides with the existing a column.
    let spec = MigrationSpec::new(ids(&["a", "b"]), ids(&["a"]))
        .rename(b"b", b"a");
    assert_eq!(
        spec.plan(),
        Err(MigrateError::ConflictingSources {
            new_id: b"a".to_vec(),
            old_ids: vec![b"a".to_vec(), b"b".to_vec()],
        })
    );
}

#[test]
fn migrate_unknown_rename_rejected() {
    let spec = MigrationSpec::new(ids(&["a"]), ids(&["a"]))
        .rename(b"zz", b"a");
    assert_eq!(
        spec.plan(),
        Err(MigrateError::UnknownRenameSource(b"zz".to_vec()))
    );
    let spec = MigrationSpec::new(ids(&["a"]), ids(&["a"]))
        .rename(b"a", b"zz");
    assert_eq!(
        spec.plan(),
        Err(MigrateError::UnknownRenameTarget(b"zz".to_vec()))
    );
}
