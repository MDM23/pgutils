use std::fmt::Display;
use std::ops::Deref;

use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, RowStream};

#[derive(Default)]
pub struct Query {
    args: Vec<Box<dyn ToSql>>,
    args_count: u16,
    buffer: String,
    separated: bool,
}

impl Query {
    pub fn new<F>(frag: F) -> Self
    where
        F: Fragment,
    {
        let mut query = Self::empty();
        frag.push_to_query(&mut query);
        query
    }

    pub fn empty() -> Self {
        Default::default()
    }

    pub fn push<F>(&mut self, frag: F) -> &mut Self
    where
        F: Fragment,
    {
        self.separated = false;

        if !self.buffer.is_empty() {
            self.buffer.push(' ');
        }

        frag.push_to_query(self);
        self
    }

    pub fn and<F>(&mut self, frag: F) -> &mut Self
    where
        F: Fragment,
    {
        if self.separated {
            self.buffer.push_str(" AND ");
        } else if !self.buffer.is_empty() {
            self.buffer.push(' ');
        }

        frag.push_to_query(self);
        self.separated = true;
        self
    }

    pub fn comma<F>(&mut self, frag: F) -> &mut Self
    where
        F: Fragment,
    {
        if self.separated {
            self.buffer.push(',');
        } else if !self.buffer.is_empty() {
            self.buffer.push(' ');
        }

        frag.push_to_query(self);
        self.separated = true;
        self
    }

    pub fn or<F>(&mut self, frag: F) -> &mut Self
    where
        F: Fragment,
    {
        if self.separated {
            self.buffer.push_str(" OR ");
        } else if !self.buffer.is_empty() {
            self.buffer.push(' ');
        }

        frag.push_to_query(self);
        self.separated = true;
        self
    }

    pub async fn run(self, client: &Client) -> Result<RowStream, tokio_postgres::Error> {
        client
            .query_raw(&self.buffer, self.args.iter().map(Deref::deref))
            .await
    }

    pub async fn execute(self, client: &Client) -> Result<u64, tokio_postgres::Error> {
        client
            .execute_raw(&self.buffer, self.args.iter().map(Deref::deref))
            .await
    }

    fn append_buffer(&mut self, query: &str) {
        // TODO: Just store the index position of the parameters to enable $n
        // style and concatenation of queries
        for c in query.chars() {
            if c == '?' {
                self.args_count += 1;
                self.buffer.push_str(&format!("${}", self.args_count));
            } else {
                self.buffer.push(c);
            }
        }
    }
}

impl Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.buffer)
    }
}

pub trait Fragment {
    fn push_to_query(self, query: &mut Query);
}

impl Fragment for &str {
    fn push_to_query(self, query: &mut Query) {
        query.append_buffer(self);
    }
}

impl<T: ToSql + 'static> Fragment for (&str, T) {
    fn push_to_query(self, query: &mut Query) {
        query.append_buffer(self.0);
        query.args.push(Box::new(self.1));
    }
}

impl<T: ToSql + 'static> Fragment for (&str, T, T) {
    fn push_to_query(self, query: &mut Query) {
        query.append_buffer(self.0);
        query.args.push(Box::new(self.1));
        query.args.push(Box::new(self.2));
    }
}

impl<T: ToSql + 'static> Fragment for (&str, T, T, T) {
    fn push_to_query(self, query: &mut Query) {
        query.append_buffer(self.0);
        query.args.push(Box::new(self.1));
        query.args.push(Box::new(self.2));
        query.args.push(Box::new(self.3));
    }
}

impl<'q, F> Fragment for F
where
    F: FnOnce(&mut Query),
{
    fn push_to_query(self, query: &mut Query) {
        let separated = query.separated;

        if separated {
            query.separated = false;
            query.buffer.push('(');
        }

        (self)(query);

        if separated {
            query.separated = true;
            query.buffer.push(')');
        }
    }
}

// TODO: This requires rewriting the positional parameters in the second query
// impl Fragment for Query {
//     fn push_to_query(self, query: &mut Query) {
//         match query.separator {
//             Some(_) => {
//                 query.buffer.push('(');
//                 query.buffer.push_str(&self.buffer);
//                 query.buffer.push(')');
//             }
//             _ => query.buffer.push_str(&self.buffer),
//         }

//         query.args_count += self.args_count;
//         query.args = query.args.drain(0..).chain(self.args.into_iter()).collect();
//     }
// }

#[test]
fn simple_query() {
    let mut query = Query::new("SELECT");
    query.comma("a");
    query.comma("b");
    query.comma("c");
    query.push("FROM foobar WHERE");
    query.and("foo = 'bar'");
    query.and(("bar = ?", 1));

    query.and(|q: &mut Query| {
        q.or(("d = ?", 10));
        q.or(("e != ?", 20));
    });

    assert_eq!(
        query.to_string(),
        "SELECT a,b,c FROM foobar WHERE foo = 'bar' AND bar = $1 AND ( d = $2 OR e != $3)"
    );
}
