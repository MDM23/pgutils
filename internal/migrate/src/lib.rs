use fs::DirEntry;
use lazy_static::lazy_static;
use proc_macro2::TokenStream;
use quote::ToTokens;
use quote::{quote, TokenStreamExt};
use regex::Regex;
use sha2::{Digest, Sha256};
use std::convert::TryFrom;
use std::fs;
use thiserror::Error;
use tokio_postgres::{Client, Row};

lazy_static! {
    static ref FILENAME_REGEX: Regex =
        Regex::new(r"^(?P<version>[0-9]+)_(?P<name>[a-z_]+)\.sql$").unwrap();
}

#[derive(Error, Debug)]
pub enum MigrationError {
    #[error("Filename is invalid")]
    FilenameError,

    #[error("Checksum of already applied migration does not match")]
    ChecksumError,

    #[error(transparent)]
    PostgresError(#[from] tokio_postgres::Error),

    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error(transparent)]
    IOError(#[from] std::io::Error),
}

#[derive(Debug)]
pub struct Migration {
    pub checksum: String,
    pub name: String,
    pub sql: String,
    pub version: i64,
}

impl TryFrom<DirEntry> for Migration {
    type Error = MigrationError;

    fn try_from(entry: DirEntry) -> Result<Self, Self::Error> {
        let file_name_os = entry.file_name();
        let file_name = file_name_os.to_str().ok_or(MigrationError::FilenameError)?;

        let cap = FILENAME_REGEX
            .captures(file_name)
            .ok_or(MigrationError::FilenameError)?;

        let name = cap
            .name("name")
            .map(|name| name.as_str())
            .ok_or(MigrationError::FilenameError)?
            .to_owned();

        let version = cap
            .name("version")
            .map(|version| version.as_str())
            .ok_or(MigrationError::FilenameError)?
            .parse()?;

        let sql = fs::read_to_string(&entry.path())?;
        let checksum = format!("{:x}", Sha256::digest(sql.as_bytes()));

        Ok(Self {
            checksum,
            name,
            sql,
            version,
        })
    }
}

impl ToTokens for Migration {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let Migration {
            checksum,
            name,
            sql,
            version,
        } = &self;

        let ts = quote! {
            pgutils::migrate::Migration {
                checksum: String::from(#checksum),
                name: String::from(#name),
                sql: String::from(#sql),
                version: #version,
            }
        };

        tokens.append_all(ts.into_iter());
    }
}

struct AppliedMigration {
    checksum: String,
    version: i64,
}

pub struct Migrator {
    pub migrations: Vec<Migration>,
}

impl Migrator {
    pub fn new(migrations: Vec<Migration>) -> Self {
        Migrator { migrations }
    }

    pub async fn migrate(&self, db: &mut Client) -> Result<(), MigrationError> {
        self.ensure_table(db).await?;

        let current = self.get_applied_migrations(db).await?;

        for migration in &self.migrations {
            match current.iter().find(|a| a.version == migration.version) {
                None => self.apply_migration(db, migration).await?,
                Some(a) => {
                    if a.checksum != migration.checksum {
                        return Err(MigrationError::ChecksumError);
                    }
                }
            };
        }

        Ok(())
    }

    async fn ensure_table(&self, db: &Client) -> Result<u64, tokio_postgres::Error> {
        db.execute(
            r#"
                CREATE TABLE IF NOT EXISTS migrations (
                    version     BIGINT PRIMARY KEY,
                    name        TEXT NOT NULL,
                    checksum    VARCHAR(64),
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
                )
            "#,
            &[],
        )
        .await
    }

    async fn get_applied_migrations(
        &self,
        db: &Client,
    ) -> Result<Vec<AppliedMigration>, tokio_postgres::Error> {
        let mut result: Vec<AppliedMigration> = vec![];

        db.query(
            r#"
                SELECT version, checksum
                FROM migrations
                ORDER BY version
            "#,
            &[],
        )
        .await?
        .iter()
        .try_for_each(|row: &Row| -> Result<(), tokio_postgres::Error> {
            result.push(AppliedMigration {
                checksum: row.try_get("checksum")?,
                version: row.try_get("version")?,
            });

            Ok(())
        })?;

        Ok(result)
    }

    async fn apply_migration(
        &self,
        db: &mut Client,
        migration: &Migration,
    ) -> Result<(), tokio_postgres::Error> {
        let tx = db.transaction().await?;

        for stmt in migration.sql.split(";") {
            if !stmt.trim().is_empty() {
                tx.execute(&stmt.to_string(), &[]).await?;
            }
        }

        tx.execute(
            r#"
                INSERT INTO migrations ( version, name, checksum )
                VALUES ($1, $2, $3)
            "#,
            &[&migration.version, &migration.name, &migration.checksum],
        )
        .await?;

        tx.commit().await
    }
}
