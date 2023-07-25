mod database;

pub mod query;

pub mod migrate {
    pub use pgutils_macros::embed;
    pub use pgutils_migrate::*;
}

pub use database::{Database, DatabaseError};

#[macro_export]
macro_rules! migrate {
    ($path: literal, $connection: expr) => {
        pgutils::migrate::embed!($path).migrate($connection)
    };
}
