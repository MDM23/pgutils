use std::fmt::{Display, Write};

use tokio_postgres::types::ToSql;

#[derive(Default)]
pub struct Query {
    args: Vec<Box<dyn ToSql>>,
    arg_indexes: Vec<usize>,
    buffer: String,
    cursor: usize,
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
            self.append_buffer(" ");
        }

        frag.push_to_query(self);
        self
    }

    pub fn and<F>(&mut self, frag: F) -> &mut Self
    where
        F: Fragment,
    {
        if self.separated {
            self.append_buffer(" AND ");
        } else if !self.buffer.is_empty() {
            self.append_buffer(" ");
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
            self.append_buffer(",");
        } else if !self.buffer.is_empty() {
            self.append_buffer(" ");
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
            self.append_buffer(" OR ");
        } else if !self.buffer.is_empty() {
            self.append_buffer(" ");
        }

        frag.push_to_query(self);
        self.separated = true;
        self
    }

    pub fn into_args(self) -> Vec<Box<dyn ToSql>> {
        self.args
    }

    fn append_buffer(&mut self, query: &str) {
        self.append_buffer_with_args(query, vec![]);
    }

    fn append_buffer_with_args(&mut self, query: &str, mut args: Vec<Box<dyn ToSql>>) {
        for c in query.chars() {
            if c == '?' {
                self.arg_indexes.push(self.cursor);
            } else {
                self.buffer.push(c);
                self.cursor += c.len_utf8();
            }
        }

        self.args.append(&mut args);
    }
}

impl Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut c = 1;

        for a in [&[0], self.arg_indexes.as_slice(), &[self.buffer.len()]]
            .concat()
            .windows(2)
        {
            if a[0] != 0 {
                f.write_char('$')?;
                f.write_str(&c.to_string())?;
                c += 1;
            }

            f.write_str(&self.buffer[a[0]..a[1]])?;
        }

        Ok(())
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
        query.append_buffer_with_args(self.0, vec![Box::new(self.1)]);
    }
}

impl<T: ToSql + 'static> Fragment for (&str, T, T) {
    fn push_to_query(self, query: &mut Query) {
        query.append_buffer_with_args(self.0, vec![Box::new(self.1), Box::new(self.2)]);
    }
}

impl<T: ToSql + 'static> Fragment for (&str, T, T, T) {
    fn push_to_query(self, query: &mut Query) {
        query.append_buffer_with_args(
            self.0,
            vec![Box::new(self.1), Box::new(self.2), Box::new(self.3)],
        );
    }
}

impl<F> Fragment for F
where
    F: FnOnce(&mut Query),
{
    fn push_to_query(self, query: &mut Query) {
        let separated = query.separated;

        if separated {
            query.separated = false;
            query.append_buffer("(");
        }

        (self)(query);

        if separated {
            query.separated = true;
            query.append_buffer(")");
        }
    }
}

impl Fragment for Query {
    fn push_to_query(mut self, query: &mut Query) {
        query.args.append(&mut self.args);

        if query.separated {
            query.append_buffer("(");
        }

        query
            .arg_indexes
            .extend(self.arg_indexes.into_iter().map(|i| i + query.cursor));

        query.cursor += self.cursor;

        query.append_buffer(&self.buffer);

        if query.separated {
            query.append_buffer(")");
        }
    }
}

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

#[test]
fn query_concatenation() {
    let mut query_a = Query::empty();

    query_a.push(|query: &mut Query| {
        query.or(("c = ?", 3));
        query.or("d = 4");
        query.or(("e = ?", 4));
    });

    let mut query_b = Query::new("SELECT * FROM test WHERE");
    query_b.and(("a = ?", 1));
    query_b.and(("b = ?", 2));
    query_b.and(query_a);

    assert_eq!(
        query_b.to_string(),
        "SELECT * FROM test WHERE a = $1 AND b = $2 AND (c = $3 OR d = 4 OR e = $4)"
    );

    let mut fields = Query::empty();
    fields.comma("foo");
    fields.comma("bar");
    fields.comma(("? as foobar", 1000));

    let mut query = Query::new("SELECT");
    query.push(fields);
    query.push("FROM my_table");

    assert_eq!(
        query.to_string(),
        "SELECT foo,bar,$1 as foobar FROM my_table"
    );
}
