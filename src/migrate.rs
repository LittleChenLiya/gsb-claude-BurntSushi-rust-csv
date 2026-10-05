//! Explicit column-description migration for [`ByteRecord`]s.
//!
//! This module implements a small, deterministic planner that rewrites the
//! fields of an old [`ByteRecord`] into a new record according to two
//! explicit column descriptions: the old and the new column ID lists, plus
//! an optional rename mapping.
//!
//! # Identity
//!
//! Columns are identified by stable numeric IDs ([`ColumnId`]). Display
//! names are never used for matching; they are only carried along for
//! diagnostics and for the textual format of the plan.
//!
//! # Planning versus execution
//!
//! Planning ([`MigrationPlanner::plan`]) never touches any record. It
//! validates the column descriptions and produces a [`MigrationPlan`] that
//! describes, for every new field, where its bytes come from:
//!
//! * [`FieldSource::Old`] — copy bytes from an old field;
//! * [`FieldSource::Default`] — fill with the default bytes of the column;
//! * [`FieldSource::Empty`] — fill with an empty field.
//!
//! Default bytes and empty fields are recorded as distinct sources so that
//! the provenance of every new field is explicit.
//!
//! Execution ([`MigrationPlan::execute`]) first validates the width of the
//! old record and then builds the new record in a fresh [`ByteRecord`]. It
//! either returns the complete new record or a [`MigrationError`]; it
//! never returns half of a new record.
//!
//! # Deletions
//!
//! An old column that is absent from the new schema is a *deletion*.
//! Deletions must be explicitly allowed via
//! [`DeletionPolicy::Allow`]; otherwise planning fails with
//! [`MigrationError::DeletionForbidden`].
//!
//! # Example
//!
//! ```
//! use csv::{ByteRecord, migrate::*};
//!
//! let old = vec![
//!     ColumnDesc::new(1, "a"),
//!     ColumnDesc::new(2, "b"),
//! ];
//! let new = vec![
//!     ColumnDesc::new(2, "b"),
//!     ColumnDesc::new(3, "c").with_default(b"x"),
//!     ColumnDesc::new(1, "a"),
//! ];
//! let plan = MigrationPlanner::new(old, new)
//!     .deletion_policy(DeletionPolicy::Allow)
//!     .plan()
//!     .unwrap();
//! let record = ByteRecord::from(vec!["1", "2"]);
//! let migrated = plan.execute(&record).unwrap();
//! assert_eq!(migrated, ByteRecord::from(vec!["2", "x", "1"]));
//! ```

use std::collections::BTreeMap;
use std::error;
use std::fmt;

use crate::byte_record::ByteRecord;

/// A stable column identifier.
///
/// Columns are matched by ID only; display names never participate in
/// identity.
pub type ColumnId = u64;

/// An explicit column description used by the migration planner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDesc {
    /// Stable identifier of the column.
    pub id: ColumnId,
    /// Human-readable display name.
    ///
    /// Used only for diagnostics and the textual format of the plan; never
    /// for identity.
    pub name: String,
    /// Whether the column is required.
    ///
    /// A required new column without a source or a default is rejected at
    /// plan time.
    pub required: bool,
    /// Default bytes used when the column is new and has no source.
    pub default: Option<Vec<u8>>,
}

impl ColumnDesc {
    /// Create a new required column description with the given ID and
    /// display name and no default.
    pub fn new(id: ColumnId, name: impl Into<String>) -> ColumnDesc {
        ColumnDesc {
            id,
            name: name.into(),
            required: true,
            default: None,
        }
    }

    /// Mark the column as optional (not required).
    ///
    /// An optional new column without a source or a default is filled with
    /// an empty field.
    pub fn optional(mut self) -> ColumnDesc {
        self.required = false;
        self
    }

    /// Set the default bytes of the column.
    ///
    /// The default is used when the column is new and has no source.
    pub fn with_default(mut self, default: &[u8]) -> ColumnDesc {
        self.default = Some(default.to_vec());
        self
    }
}

/// Policy governing columns that disappear in the new schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum DeletionPolicy {
    /// Refuse to plan if any old column is absent from the new schema.
    ///
    /// This is the default.
    #[default]
    Forbid,
    /// Allow old columns to be dropped.
    Allow,
}

/// Where the bytes of a new field come from.
///
/// The source is recorded per new field so that default bytes and empty
/// fields remain distinguishable in the plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldSource {
    /// Copy bytes from the old record field at this index.
    Old(usize),
    /// Fill with these default bytes (a new column with a default).
    Default(Vec<u8>),
    /// Fill with an empty field (a new optional column without a default).
    Empty,
}

/// An error returned while planning or executing a migration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MigrationError {
    /// The old column ID list contains a duplicate ID.
    DuplicateOldColumn(ColumnId),
    /// The new (target) column ID list contains a duplicate ID.
    DuplicateTarget(ColumnId),
    /// A rename mapping references an old column ID that does not exist.
    UnknownRename(ColumnId),
    /// A rename mapping would make two old columns map to one new column.
    RenameConflict {
        /// The old column ID being renamed.
        old: ColumnId,
        /// The new column ID that is already sourced.
        new: ColumnId,
    },
    /// A required new column has neither a source nor a default.
    MissingRequiredDefault(ColumnId),
    /// An old column would be deleted but the deletion policy forbids it.
    DeletionForbidden(Vec<ColumnId>),
    /// The width of the old record does not match the planned old width.
    WidthMismatch {
        /// Expected number of old fields.
        expected: usize,
        /// Actual number of old fields.
        got: usize,
    },
}

impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MigrationError::DuplicateOldColumn(id) => {
                write!(f, "duplicate old column id {id}")
            }
            MigrationError::DuplicateTarget(id) => {
                write!(f, "duplicate target column id {id}")
            }
            MigrationError::UnknownRename(id) => {
                write!(f, "rename references unknown old column id {id}")
            }
            MigrationError::RenameConflict { old, new } => {
                write!(
                    f,
                    "rename of old column id {old} to new column id {new} \
                     conflicts with an existing source"
                )
            }
            MigrationError::MissingRequiredDefault(id) => {
                write!(
                    f,
                    "required new column id {id} has no source and no default"
                )
            }
            MigrationError::DeletionForbidden(ids) => {
                write!(
                    f,
                    "deletion of old column(s) {} is forbidden by policy",
                    Ids(ids)
                )
            }
            MigrationError::WidthMismatch { expected, got } => {
                write!(
                    f,
                    "old record width mismatch: expected {expected} field(s), \
                     got {got}"
                )
            }
        }
    }
}

impl error::Error for MigrationError {}

/// Formats a list of column IDs for diagnostics.
struct Ids<'a>(&'a [ColumnId]);

impl fmt::Display for Ids<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.0).finish()
    }
}

/// Builds a [`MigrationPlan`] from explicit old and new column descriptions.
///
/// By default, deletions are forbidden and no renames are declared.
#[derive(Clone, Debug)]
pub struct MigrationPlanner {
    old: Vec<ColumnDesc>,
    new: Vec<ColumnDesc>,
    renames: Vec<(ColumnId, ColumnId)>,
    deletion_policy: DeletionPolicy,
}

impl MigrationPlanner {
    /// Create a planner for the given old and new column descriptions.
    ///
    /// Columns are matched by ID; display names are irrelevant.
    pub fn new(old: Vec<ColumnDesc>, new: Vec<ColumnDesc>) -> MigrationPlanner {
        MigrationPlanner {
            old,
            new,
            renames: Vec::new(),
            deletion_policy: DeletionPolicy::Forbid,
        }
    }

    /// Declare that old column `old` is the same column as new column `new`
    /// (its identity was preserved across an ID change).
    ///
    /// The rename is ignored if `old` and `new` are equal.
    pub fn rename(mut self, old: ColumnId, new: ColumnId) -> MigrationPlanner {
        self.renames.push((old, new));
        self
    }

    /// Set the deletion policy.
    ///
    /// Deletions are forbidden by default.
    pub fn deletion_policy(
        mut self,
        policy: DeletionPolicy,
    ) -> MigrationPlanner {
        self.deletion_policy = policy;
        self
    }

    /// Produce the field transfer plan.
    ///
    /// Planning never inspects any record; it validates the column
    /// descriptions and resolves every new field's source.
    pub fn plan(&self) -> Result<MigrationPlan, MigrationError> {
        // 1. Reject duplicate IDs in the new (target) schema first.
        let mut seen_new = BTreeMap::new();
        for col in &self.new {
            if seen_new.insert(col.id, ()).is_some() {
                return Err(MigrationError::DuplicateTarget(col.id));
            }
        }
        // 2. Reject duplicate IDs in the old schema.
        let mut old_index = BTreeMap::new();
        for (i, col) in self.old.iter().enumerate() {
            if old_index.insert(col.id, i).is_some() {
                return Err(MigrationError::DuplicateOldColumn(col.id));
            }
        }
        // 3. Validate renames and build a map of new ID -> old index.
        let mut rename_sources = BTreeMap::new();
        for &(old_id, new_id) in &self.renames {
            let &idx = old_index
                .get(&old_id)
                .ok_or(MigrationError::UnknownRename(old_id))?;
            if old_id == new_id {
                // Identity rename: a no-op.
                continue;
            }
            // The new ID must not already be sourced from the old schema.
            if old_index.contains_key(&new_id) {
                return Err(MigrationError::RenameConflict {
                    old: old_id,
                    new: new_id,
                });
            }
            if rename_sources.insert(new_id, idx).is_some() {
                return Err(MigrationError::RenameConflict {
                    old: old_id,
                    new: new_id,
                });
            }
        }
        // 4. Resolve each new field's source.
        let mut steps = Vec::with_capacity(self.new.len());
        let mut used_old = vec![false; self.old.len()];
        for col in &self.new {
            let source = if let Some(&idx) = old_index.get(&col.id) {
                used_old[idx] = true;
                FieldSource::Old(idx)
            } else if let Some(&idx) = rename_sources.get(&col.id) {
                used_old[idx] = true;
                FieldSource::Old(idx)
            } else if let Some(default) = &col.default {
                FieldSource::Default(default.clone())
            } else if col.required {
                return Err(MigrationError::MissingRequiredDefault(col.id));
            } else {
                FieldSource::Empty
            };
            steps.push(source);
        }
        // 5. Deletions: old columns that were never sourced.
        let dropped: Vec<ColumnId> = self
            .old
            .iter()
            .enumerate()
            .filter(|(i, _)| !used_old[*i])
            .map(|(_, c)| c.id)
            .collect();
        if !dropped.is_empty()
            && self.deletion_policy == DeletionPolicy::Forbid
        {
            return Err(MigrationError::DeletionForbidden(dropped));
        }
        Ok(MigrationPlan {
            old_width: self.old.len(),
            steps,
            dropped,
            new_ids: self.new.iter().map(|c| c.id).collect(),
            new_names: self.new.iter().map(|c| c.name.clone()).collect(),
        })
    }
}

/// A validated field transfer plan.
///
/// The plan records, for every new field, the exact source of its bytes.
/// Default bytes and empty fields are recorded as distinct sources
/// ([`FieldSource::Default`] versus [`FieldSource::Empty`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationPlan {
    old_width: usize,
    steps: Vec<FieldSource>,
    dropped: Vec<ColumnId>,
    new_ids: Vec<ColumnId>,
    new_names: Vec<String>,
}

impl MigrationPlan {
    /// Number of fields in the old schema.
    pub fn old_width(&self) -> usize {
        self.old_width
    }

    /// Number of fields in the new schema.
    pub fn new_width(&self) -> usize {
        self.steps.len()
    }

    /// Per-field sources for the new record, in new-schema order.
    pub fn steps(&self) -> &[FieldSource] {
        &self.steps
    }

    /// Old column IDs that are dropped by this migration.
    pub fn dropped(&self) -> &[ColumnId] {
        &self.dropped
    }

    /// Execute the plan against `record`.
    ///
    /// The width of the record must match the planned old width; otherwise
    /// [`MigrationError::WidthMismatch`] is returned. The new record is
    /// built in a fresh [`ByteRecord`] and only returned on success, so a
    /// failed execution never yields half of a new record.
    pub fn execute(
        &self,
        record: &ByteRecord,
    ) -> Result<ByteRecord, MigrationError> {
        if record.len() != self.old_width {
            return Err(MigrationError::WidthMismatch {
                expected: self.old_width,
                got: record.len(),
            });
        }
        let mut out =
            ByteRecord::with_capacity(record.as_slice().len(), self.steps.len());
        for step in &self.steps {
            match step {
                FieldSource::Old(i) => {
                    out.push_field(record.get(*i).expect("width was checked"));
                }
                FieldSource::Default(bytes) => out.push_field(bytes),
                FieldSource::Empty => out.push_field(&[]),
            }
        }
        out.set_position(record.position().cloned());
        Ok(out)
    }
}

impl fmt::Display for MigrationPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "migration plan: {} old field(s) -> {} new field(s)",
            self.old_width,
            self.steps.len()
        )?;
        for (i, step) in self.steps.iter().enumerate() {
            let id = self.new_ids.get(i).copied().unwrap_or(0);
            let name = self.new_names.get(i).map(|s| s.as_str()).unwrap_or("?");
            match step {
                FieldSource::Old(j) => {
                    writeln!(
                        f,
                        "  [{i}] <- old[{j}]            id {id:<3} \"{}\"",
                        Escape(name)
                    )?;
                }
                FieldSource::Default(bytes) => {
                    writeln!(
                        f,
                        "  [{i}] <- default {}  id {id:<3} \"{}\"",
                        ByteStr(bytes),
                        Escape(name)
                    )?;
                }
                FieldSource::Empty => {
                    writeln!(
                        f,
                        "  [{i}] <- empty              id {id:<3} \"{}\"",
                        Escape(name)
                    )?;
                }
            }
        }
        if self.dropped.is_empty() {
            writeln!(f, "  dropped: (none)")?;
        } else {
            writeln!(f, "  dropped: {}", Ids(&self.dropped))?;
        }
        Ok(())
    }
}

/// Formats a byte string as a Rust-style byte string literal.
struct ByteStr<'a>(&'a [u8]);

impl fmt::Display for ByteStr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "b\"")?;
        for &b in self.0 {
            match b {
                b'"' => write!(f, "\\\"")?,
                b'\\' => write!(f, "\\\\")?,
                b'\n' => write!(f, "\\n")?,
                b'\r' => write!(f, "\\r")?,
                b'\t' => write!(f, "\\t")?,
                0x20..=0x7e => write!(f, "{}", b as char)?,
                _ => write!(f, "\\x{b:02x}")?,
            }
        }
        write!(f, "\"")
    }
}

/// Formats a string with Rust-style escaping for quotes and backslashes.
struct Escape<'a>(&'a str);

impl fmt::Display for Escape<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for c in self.0.chars() {
            match c {
                '"' => write!(f, "\\\"")?,
                '\\' => write!(f, "\\\\")?,
                _ => write!(f, "{c}")?,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn old_schema() -> Vec<ColumnDesc> {
        vec![
            ColumnDesc::new(1, "a"),
            ColumnDesc::new(2, "b"),
        ]
    }

    fn new_schema() -> Vec<ColumnDesc> {
        vec![
            ColumnDesc::new(2, "b"),
            ColumnDesc::new(3, "c").with_default(b"x"),
            ColumnDesc::new(1, "a"),
        ]
    }

    #[test]
    fn plan_and_execute_basic() {
        let plan = MigrationPlanner::new(old_schema(), new_schema())
            .deletion_policy(DeletionPolicy::Allow)
            .plan()
            .unwrap();
        assert_eq!(plan.old_width(), 2);
        assert_eq!(plan.new_width(), 3);
        assert_eq!(
            plan.steps(),
            &[
                FieldSource::Old(1),
                FieldSource::Default(b"x".to_vec()),
                FieldSource::Old(0),
            ]
        );
        assert_eq!(plan.dropped(), &[] as &[ColumnId]);

        let record = ByteRecord::from(vec!["1", "2"]);
        let migrated = plan.execute(&record).unwrap();
        assert_eq!(migrated, ByteRecord::from(vec!["2", "x", "1"]));
    }

    #[test]
    fn plan_display_format() {
        let plan = MigrationPlanner::new(old_schema(), new_schema())
            .deletion_policy(DeletionPolicy::Allow)
            .plan()
            .unwrap();
        let expected = "\
migration plan: 2 old field(s) -> 3 new field(s)
  [0] <- old[1]            id 2   \"b\"
  [1] <- default b\"x\"  id 3   \"c\"
  [2] <- old[0]            id 1   \"a\"
  dropped: (none)
";
        assert_eq!(plan.to_string(), expected);
    }

    #[test]
    fn duplicate_target_rejected() {
        let new = vec![
            ColumnDesc::new(2, "b"),
            ColumnDesc::new(2, "b2"),
            ColumnDesc::new(1, "a"),
        ];
        let err = MigrationPlanner::new(old_schema(), new)
            .deletion_policy(DeletionPolicy::Allow)
            .plan()
            .unwrap_err();
        assert_eq!(err, MigrationError::DuplicateTarget(2));
    }

    #[test]
    fn duplicate_old_rejected() {
        let old = vec![
            ColumnDesc::new(1, "a"),
            ColumnDesc::new(1, "a2"),
        ];
        let err = MigrationPlanner::new(old, new_schema())
            .deletion_policy(DeletionPolicy::Allow)
            .plan()
            .unwrap_err();
        assert_eq!(err, MigrationError::DuplicateOldColumn(1));
    }

    #[test]
    fn missing_required_default_rejected() {
        let new = vec![
            ColumnDesc::new(2, "b"),
            ColumnDesc::new(3, "c"),
            ColumnDesc::new(1, "a"),
        ];
        let err = MigrationPlanner::new(old_schema(), new)
            .deletion_policy(DeletionPolicy::Allow)
            .plan()
            .unwrap_err();
        assert_eq!(err, MigrationError::MissingRequiredDefault(3));
    }

    #[test]
    fn optional_new_column_without_default_is_empty() {
        let new = vec![
            ColumnDesc::new(2, "b"),
            ColumnDesc::new(3, "c").optional(),
            ColumnDesc::new(1, "a"),
        ];
        let plan = MigrationPlanner::new(old_schema(), new)
            .deletion_policy(DeletionPolicy::Allow)
            .plan()
            .unwrap();
        assert_eq!(
            plan.steps(),
            &[FieldSource::Old(1), FieldSource::Empty, FieldSource::Old(0)]
        );
        let record = ByteRecord::from(vec!["1", "2"]);
        let migrated = plan.execute(&record).unwrap();
        assert_eq!(migrated, ByteRecord::from(vec!["2", "", "1"]));
    }

    #[test]
    fn deletion_forbidden_by_default() {
        let old = vec![
            ColumnDesc::new(1, "a"),
            ColumnDesc::new(2, "b"),
            ColumnDesc::new(3, "d"),
        ];
        // New schema drops column 3.
        let new = vec![
            ColumnDesc::new(2, "b"),
            ColumnDesc::new(1, "a"),
        ];
        let err = MigrationPlanner::new(old, new).plan().unwrap_err();
        assert_eq!(err, MigrationError::DeletionForbidden(vec![3]));
    }

    #[test]
    fn deletion_allowed_with_policy() {
        let old = vec![
            ColumnDesc::new(1, "a"),
            ColumnDesc::new(2, "b"),
            ColumnDesc::new(3, "d"),
        ];
        // New schema drops column 3.
        let new = vec![
            ColumnDesc::new(2, "b"),
            ColumnDesc::new(1, "a"),
        ];
        let plan = MigrationPlanner::new(old, new)
            .deletion_policy(DeletionPolicy::Allow)
            .plan()
            .unwrap();
        assert_eq!(plan.dropped(), &[3]);
        let record = ByteRecord::from(vec!["1", "2", "3"]);
        let migrated = plan.execute(&record).unwrap();
        assert_eq!(migrated, ByteRecord::from(vec!["2", "1"]));
    }

    #[test]
    fn rename_mapping_resources_column() {
        // Old column 1 ("a") was renamed to new id 7.
        let old = vec![
            ColumnDesc::new(1, "a"),
            ColumnDesc::new(2, "b"),
        ];
        let new = vec![
            ColumnDesc::new(7, "a"),
            ColumnDesc::new(2, "b"),
        ];
        let plan = MigrationPlanner::new(old, new).rename(1, 7).plan().unwrap();
        assert_eq!(plan.steps(), &[FieldSource::Old(0), FieldSource::Old(1)]);
        let record = ByteRecord::from(vec!["1", "2"]);
        let migrated = plan.execute(&record).unwrap();
        assert_eq!(migrated, ByteRecord::from(vec!["1", "2"]));
    }

    #[test]
    fn rename_unknown_old_id_rejected() {
        let err = MigrationPlanner::new(old_schema(), new_schema())
            .rename(99, 3)
            .plan()
            .unwrap_err();
        assert_eq!(err, MigrationError::UnknownRename(99));
    }

    #[test]
    fn rename_conflict_rejected() {
        // New id 3 already exists in the old schema, so the rename would
        // source the same new field twice.
        let old = vec![
            ColumnDesc::new(1, "a"),
            ColumnDesc::new(2, "b"),
            ColumnDesc::new(3, "c"),
        ];
        let err = MigrationPlanner::new(old, new_schema())
            .rename(1, 3)
            .plan()
            .unwrap_err();
        assert_eq!(err, MigrationError::RenameConflict { old: 1, new: 3 });
    }

    #[test]
    fn width_mismatch_rejected_at_execution() {
        let plan = MigrationPlanner::new(old_schema(), new_schema())
            .deletion_policy(DeletionPolicy::Allow)
            .plan()
            .unwrap();
        let record = ByteRecord::from(vec!["1"]);
        let err = plan.execute(&record).unwrap_err();
        assert_eq!(
            err,
            MigrationError::WidthMismatch { expected: 2, got: 1 }
        );
        // A failed execution must not produce a partial record.
        assert_eq!(record.len(), 1);
    }

    #[test]
    fn default_bytes_are_not_empty_fields() {
        let plan = MigrationPlanner::new(old_schema(), new_schema())
            .deletion_policy(DeletionPolicy::Allow)
            .plan()
            .unwrap();
        // The plan distinguishes default bytes from an empty field.
        assert!(matches!(plan.steps()[1], FieldSource::Default(_)));
        assert!(!matches!(plan.steps()[1], FieldSource::Empty));
    }

    #[test]
    fn empty_record_migrates_when_all_optional() {
        let old: Vec<ColumnDesc> = vec![];
        let new = vec![
            ColumnDesc::new(3, "c").optional(),
            ColumnDesc::new(4, "d").with_default(b"z"),
        ];
        let plan = MigrationPlanner::new(old, new).plan().unwrap();
        let record = ByteRecord::new();
        let migrated = plan.execute(&record).unwrap();
        assert_eq!(migrated, ByteRecord::from(vec!["", "z"]));
    }
}
