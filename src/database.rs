use std::ops::Deref;

use futures_util::{pin_mut, TryStreamExt};
use qp_postgres::PgPool;
use thiserror::Error;
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    types::ToSql,
    Row, Socket, ToStatement,
};

use crate::query::Query;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Query returned an unexpected number of rows")]
    EmptyResult,

    #[error(transparent)]
    PostgresError(#[from] tokio_postgres::Error),
}

#[derive(Clone)]
pub struct Database<P>
where
    P: MakeTlsConnect<Socket> + Clone + Send + Sync,
    P::Stream: Send + Sync + 'static,
    P::TlsConnect: Send + Sync,
    <P::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pool: PgPool<P>,
}

impl<P> Database<P>
where
    P: MakeTlsConnect<Socket> + Clone + Send + Sync,
    P::Stream: Send + Sync + 'static,
    P::TlsConnect: Send + Sync,
    <P::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub fn new(pool: PgPool<P>) -> Self {
        Self { pool }
    }

    pub async fn query<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<impl Iterator<Item = Row>, DatabaseError>
    where
        T: ?Sized + ToStatement,
    {
        Ok(self
            .pool
            .acquire()
            .await?
            .query_raw(statement, slice_iter(params))
            .await?
            .try_collect::<Vec<Row>>()
            .await?
            .into_iter())
    }

    pub async fn query_one<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, DatabaseError>
    where
        T: ?Sized + ToStatement,
    {
        let stream = self
            .pool
            .acquire()
            .await?
            .query_raw(statement, slice_iter(params))
            .await?;

        pin_mut!(stream);

        let row = match stream.try_next().await? {
            Some(row) => row,
            None => return Err(DatabaseError::EmptyResult),
        };

        if stream.try_next().await?.is_some() {
            return Err(DatabaseError::EmptyResult);
        }

        Ok(row)
    }

    pub async fn execute<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, DatabaseError>
    where
        T: ?Sized + ToStatement,
    {
        Ok(self
            .pool
            .acquire()
            .await?
            .execute_raw(statement, slice_iter(params))
            .await?)
    }
}

fn slice_iter<'a>(
    s: &'a [&'a (dyn ToSql + Sync)],
) -> impl ExactSizeIterator<Item = &'a dyn ToSql> + 'a {
    s.iter().map(|s| *s as _)
}

impl Query {
    pub async fn get<P>(self, db: &Database<P>) -> Result<impl Iterator<Item = Row>, DatabaseError>
    where
        P: MakeTlsConnect<Socket> + Clone + Send + Sync,
        P::Stream: Send + Sync + 'static,
        P::TlsConnect: Send + Sync,
        <P::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        Ok(db
            .pool
            .acquire()
            .await?
            .query_raw(&self.to_string(), self.into_args().iter().map(Deref::deref))
            .await?
            .try_collect::<Vec<Row>>()
            .await?
            .into_iter())
    }

    pub async fn get_one<P>(self, db: &Database<P>) -> Result<Row, DatabaseError>
    where
        P: MakeTlsConnect<Socket> + Clone + Send + Sync,
        P::Stream: Send + Sync + 'static,
        P::TlsConnect: Send + Sync,
        <P::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        let stream = db
            .pool
            .acquire()
            .await?
            .query_raw(&self.to_string(), self.into_args().iter().map(Deref::deref))
            .await?;

        pin_mut!(stream);

        let row = match stream.try_next().await? {
            Some(row) => row,
            None => return Err(DatabaseError::EmptyResult),
        };

        if stream.try_next().await?.is_some() {
            return Err(DatabaseError::EmptyResult);
        }

        Ok(row)
    }

    pub async fn execute<P>(self, db: &Database<P>) -> Result<u64, DatabaseError>
    where
        P: MakeTlsConnect<Socket> + Clone + Send + Sync,
        P::Stream: Send + Sync + 'static,
        P::TlsConnect: Send + Sync,
        <P::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        Ok(db
            .pool
            .acquire()
            .await?
            .execute_raw(&self.to_string(), self.into_args().iter().map(Deref::deref))
            .await?)
    }
}
