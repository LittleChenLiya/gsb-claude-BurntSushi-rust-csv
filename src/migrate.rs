//! Explicit column-ID based migration planning for [`ByteRecord`].
//!
//! This module builds a *field move plan* from an explicit description of
//! the old and new column layouts. Columns are matched by their explicit
//! column IDs (arbitrary byte strings), never by display names; any change
//! of ID between the old and new layout must be declared through an
//! explicit rename mapping.
//!
//! # Plan format
//!
//! A [`MigrationPlan`] is an ordered list of [`PlanStep`]s, one per column
//! of the new layout, in new-column order:
//!
//! * `PlanStep::Copy { old_index, old_id, new_id }` - copy the field at
//!   `old_index` of the old record (column `old_id`) into this position,
//!   which is column `new_id` in the new layout.
//! * `PlanStep::Default { new_id, bytes }` - emit the fixed default
//!   `bytes` for the brand new column `new_id`.
//!
//! The textual rendering ([`MigrationPlan::describe`]) emits one line per
//! step, e.g.:
//!
//! ```text
//! copy 0:a -> b
//! default c = "x"
//! copy 1:b -> a
//! ```
//!
//! # Safety rules
//!
//! * Duplicate IDs in either the old or the new column list are rejected.
//! * A new column with no source old column is *required*: it must carry
//!   an explicit default, otherwise planning fails.
//! * Old columns that do not feed any new column are deletions; planning
//!   fails unless the policy explicitly allows deletions.
//! * Execution validates that the old record width matches the declared
//!   old column count.
//! * Execution is all-or-nothing: on any failure no (partial) new record
//!   is produced.
//! * Provenance is recorded per output field: bytes copied from the old
//!   record (even when the copied field is empty) are reported as
//!   [`FieldOrigin::Copied`], while bytes materialized from a declared
//!   default are reported as [`FieldOrigin::Default`]. An empty copied
//!   field is therefore never confused with a default.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use crate::byte_record::ByteRecord;

/// One step of a migration plan, describing how one new column is filled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanStep {
    /// Copy the field at `old_index` of the old record into this position.
    Copy {
        /// Index of the source field in the old record.
        old_index: usize,
        /// ID of the source column in the old layout.
        old_id: Vec<u8>,
        /// ID of the target column in the new layout.
        new_id: Vec<u8>,
    },
    /// Emit a declared default for a brand new column.
    Default {
        /// ID of the new column.
        new_id: Vec<u8>,
        /// The default bytes to emit.
        bytes: Vec<u8>,
    },
}

/// Provenance of one field of a migrated record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldOrigin {
    /// The field was copied from the old record. Note that the copied
    /// bytes may be empty; an empty copied field is still `Copied`, not
    /// `Default`.
    Copied {
        /// Index of the source field in the old record.
        old_index: usize,
        /// ID of the source column in the old layout.
        old_id: Vec<u8>,
    },
    /// The field was materialized from a declared default.
    Default {
        /// ID of the new column the default belongs to.
        new_id: Vec<u8>,
    },
}

/// An ordered field move plan from an old layout to a new layout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationPlan {
    steps: Vec<PlanStep>,
    old_width: usize,
}

impl MigrationPlan {
    /// Returns the steps of this plan, in new-column order.
    pub fn steps(&self) -> &[PlanStep] {
        &self.steps
    }

    /// Returns the required width of old records consumed by this plan.
    pub fn old_width(&self) -> usize {
        self.old_width
    }

    /// Renders the plan in its textual format, one step per line.
    pub fn describe(&self) -> String {
        let mut out = String::new();
        for step in &self.steps {
            match *step {
                PlanStep::Copy { old_index, ref old_id, ref new_id } => {
                    out.push_str(&format!(
                        "copy {}:{} -> {}\n",
                        old_index,
                        String::from_utf8_lossy(old_id),
                        String::from_utf8_lossy(new_id),
                    ));
                }
                PlanStep::Default { ref new_id, ref bytes } => {
                    out.push_str(&format!(
                        "default {} = \"{}\"\n",
                        String::from_utf8_lossy(new_id),
                        String::from_utf8_lossy(bytes),
                    ));
                }
            }
        }
        out
    }

    /// Executes this plan on one old record.
    ///
    /// The old record width is validated against the declared old column
    /// count. On any error, no (partial) new record is returned.
    pub fn execute(
        &self,
        old: &ByteRecord,
    ) -> Result<MigratedRecord, MigrateError> {
        if old.len() != self.old_width {
            return Err(MigrateError::WidthMismatch {
                expected: self.old_width,
                got: old.len(),
            });
        }
        let mut record = ByteRecord::new();
        let mut origins = Vec::with_capacity(self.steps.len());
        for step in &self.steps {
            match *step {
                PlanStep::Copy { old_index, ref old_id, .. } => {
                    let field = old.get(old_index).ok_or(
                        MigrateError::WidthMismatch {
                            expected: self.old_width,
                            got: old.len(),
                        },
                    )?;
                    record.push_field(field);
                    origins.push(FieldOrigin::Copied {
                        old_index,
                        old_id: old_id.clone(),
                    });
                }
                PlanStep::Default { ref new_id, ref bytes } => {
                    record.push_field(bytes);
                    origins.push(FieldOrigin::Default {
                        new_id: new_id.clone(),
                    });
                }
            }
        }
        Ok(MigratedRecord { record, origins })
    }
}

/// The result of executing a migration plan on one record: the new
/// record plus the per-field provenance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigratedRecord {
    record: ByteRecord,
    origins: Vec<FieldOrigin>,
}

impl MigratedRecord {
    /// Returns the migrated record.
    pub fn record(&self) -> &ByteRecord {
        &self.record
    }

    /// Returns the provenance of each field, in new-column order.
    pub fn origins(&self) -> &[FieldOrigin] {
        &self.origins
    }

    /// Consumes this value and returns the migrated record.
    pub fn into_record(self) -> ByteRecord {
        self.record
    }
}

/// Errors that can occur while planning or executing a migration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MigrateError {
    /// The old column list contains the same ID twice.
    DuplicateSourceId(Vec<u8>),
    /// The new column list contains the same ID twice.
    DuplicateTargetId(Vec<u8>),
    /// A rename refers to an old column ID that does not exist.
    UnknownRenameSource(Vec<u8>),
    /// A rename targets a new column ID that does not exist.
    UnknownRenameTarget(Vec<u8>),
    /// Two old columns map to the same new column.
    ConflictingSources {
        /// The new column ID with conflicting sources.
        new_id: Vec<u8>,
        /// The old column IDs that both map to `new_id`.
        old_ids: Vec<Vec<u8>>,
    },
    /// A new column has no source old column and no declared default.
    MissingDefault(Vec<u8>),
    /// Old columns would be deleted but the policy does not allow it.
    DeletionNotAuthorized(Vec<Vec<u8>>),
    /// A default was declared for a column that already has a source.
    DefaultOnCopiedColumn(Vec<u8>),
    /// The old record width does not match the declared old layout.
    WidthMismatch {
        /// The declared old column count.
        expected: usize,
        /// The actual record width.
        got: usize,
    },
}

impl fmt::Display for MigrateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            MigrateError::DuplicateSourceId(ref id) => write!(
                f,
                "duplicate old column id: {}",
                String::from_utf8_lossy(id)
            ),
            MigrateError::DuplicateTargetId(ref id) => write!(
                f,
                "duplicate new column id: {}",
                String::from_utf8_lossy(id)
            ),
            MigrateError::UnknownRenameSource(ref id) => write!(
                f,
                "rename source is not an old column id: {}",
                String::from_utf8_lossy(id)
            ),
            MigrateError::UnknownRenameTarget(ref id) => write!(
                f,
                "rename target is not a new column id: {}",
                String::from_utf8_lossy(id)
            ),
            MigrateError::ConflictingSources { ref new_id, ref old_ids } => {
                write!(
                    f,
                    "new column id {} has conflicting sources: {}",
                    String::from_utf8_lossy(new_id),
                    old_ids
                        .iter()
                        .map(|id| String::from_utf8_lossy(id).into_owned())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            MigrateError::MissingDefault(ref id) => write!(
                f,
                "new required column has no default: {}",
                String::from_utf8_lossy(id)
            ),
            MigrateError::DeletionNotAuthorized(ref ids) => write!(
                f,
                "old columns would be deleted but deletions are not \
                 allowed by policy: {}",
                ids.iter()
                    .map(|id| String::from_utf8_lossy(id).into_owned())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            MigrateError::DefaultOnCopiedColumn(ref id) => write!(
                f,
                "default declared for column that already has a source: {}",
                String::from_utf8_lossy(id)
            ),
            MigrateError::WidthMismatch { expected, got } => write!(
                f,
                "old record width mismatch: expected {} fields, got {}",
                expected, got
            ),
        }
    }
}

impl Error for MigrateError {}

/// An explicit column migration specification.
///
/// Column IDs are arbitrary byte strings and are matched exactly; nothing
/// is inferred from display names. Renames between the old and new layout
/// must be declared explicitly via [`MigrationSpec::rename`].
#[derive(Clone, Debug, Default)]
pub struct MigrationSpec {
    old_columns: Vec<Vec<u8>>,
    new_columns: Vec<Vec<u8>>,
    renames: BTreeMap<Vec<u8>, Vec<u8>>,
    defaults: BTreeMap<Vec<u8>, Vec<u8>>,
    allow_deletions: bool,
}

impl MigrationSpec {
    /// Creates a new spec from the old and new column ID lists.
    pub fn new(
        old_columns: Vec<Vec<u8>>,
        new_columns: Vec<Vec<u8>>,
    ) -> MigrationSpec {
        MigrationSpec {
            old_columns,
            new_columns,
            renames: BTreeMap::new(),
            defaults: BTreeMap::new(),
            allow_deletions: false,
        }
    }

    /// Declares that old column `old_id` becomes new column `new_id`.
    pub fn rename(mut self, old_id: &[u8], new_id: &[u8]) -> MigrationSpec {
        self.renames.insert(old_id.to_vec(), new_id.to_vec());
        self
    }

    /// Declares a default for the brand new column `new_id`.
    pub fn default(mut self, new_id: &[u8], bytes: &[u8]) -> MigrationSpec {
        self.defaults.insert(new_id.to_vec(), bytes.to_vec());
        self
    }

    /// Sets the deletion policy. Deletions of old columns that do not
    /// feed any new column are rejected unless this is set to `true`.
    pub fn allow_deletions(mut self, yes: bool) -> MigrationSpec {
        self.allow_deletions = yes;
        self
    }

    /// Builds the field move plan, or fails without producing anything.
    pub fn plan(&self) -> Result<MigrationPlan, MigrateError> {
        // Reject duplicate IDs on both sides.
        let mut seen = BTreeSet::new();
        for id in &self.old_columns {
            if !seen.insert(id) {
                return Err(MigrateError::DuplicateSourceId(id.clone()));
            }
        }
        let mut seen = BTreeSet::new();
        for id in &self.new_columns {
            if !seen.insert(id) {
                return Err(MigrateError::DuplicateTargetId(id.clone()));
            }
        }
        // Validate renames against both layouts.
        for (old_id, new_id) in &self.renames {
            if !self.old_columns.contains(old_id) {
                return Err(MigrateError::UnknownRenameSource(old_id.clone()));
            }
            if !self.new_columns.contains(new_id) {
                return Err(MigrateError::UnknownRenameTarget(new_id.clone()));
            }
        }
        // Map each old column to its new ID (explicit rename or identity).
        let mapped: Vec<Vec<u8>> = self
            .old_columns
            .iter()
            .map(|old_id| {
                self.renames
                    .get(old_id)
                    .cloned()
                    .unwrap_or_else(|| old_id.clone())
            })
            .collect();
        // Build one plan step per new column, in new-column order.
        let mut steps = Vec::with_capacity(self.new_columns.len());
        let mut used = vec![false; self.old_columns.len()];
        for new_id in &self.new_columns {
            let sources: Vec<usize> = mapped
                .iter()
                .enumerate()
                .filter(|&(_, mapped_id)| mapped_id == new_id)
                .map(|(i, _)| i)
                .collect();
            match sources.len() {
                0 => {
                    let bytes = self
                        .defaults
                        .get(new_id)
                        .cloned()
                        .ok_or_else(|| {
                            MigrateError::MissingDefault(new_id.clone())
                        })?;
                    steps.push(PlanStep::Default {
                        new_id: new_id.clone(),
                        bytes,
                    });
                }
                1 => {
                    if self.defaults.contains_key(new_id) {
                        return Err(MigrateError::DefaultOnCopiedColumn(
                            new_id.clone(),
                        ));
                    }
                    let old_index = sources[0];
                    used[old_index] = true;
                    steps.push(PlanStep::Copy {
                        old_index,
                        old_id: self.old_columns[old_index].clone(),
                        new_id: new_id.clone(),
                    });
                }
                _ => {
                    return Err(MigrateError::ConflictingSources {
                        new_id: new_id.clone(),
                        old_ids: sources
                            .iter()
                            .map(|&i| self.old_columns[i].clone())
                            .collect(),
                    });
                }
            }
        }
        // Old columns feeding nothing are deletions and need authorization.
        let deleted: Vec<Vec<u8>> = self
            .old_columns
            .iter()
            .enumerate()
            .filter(|&(i, _)| !used[i])
            .map(|(_, id)| id.clone())
            .collect();
        if !deleted.is_empty() && !self.allow_deletions {
            return Err(MigrateError::DeletionNotAuthorized(deleted));
        }
        Ok(MigrationPlan { steps, old_width: self.old_columns.len() })
    }
}
