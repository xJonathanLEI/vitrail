<p align="center">
  <h1 align="center">vitrail</h1>
</p>

<p align="center">
  <a href="https://crates.io/crates/vitrail-pg"><img alt="crates-badge" src="https://img.shields.io/crates/v/vitrail-pg.svg"></a>
</p>

<p align="center">
  <strong>A next-generation ORM for Rust, inspired by <a href="https://github.com/prisma/prisma">Prisma</a> and <a href="https://github.com/drizzle-team/drizzle-orm">Drizzle</a>.</strong>
</p>

`vitrail` is a schema-first ORM focused on generating efficient SQL while keeping the API explicit, ergonomic, and **type-safe end to end**.

> [!NOTE]
>
> `vitrail` is under active development. Use at your own risks.
>
> The library currently only supports Postgres. _Just use Postgres™_.

## Core features

- Prisma-like syntax for running compile-time validated SQL queries and getting type-safe data
- Transaction support
- Migration script generation and management - [via the CLI](./examples/pg_migration_cli.rs) or programmatically
- [Custom Rust type support](./examples/pg_custom_string_types.rs) with arbitrary mapping logic via trait implementation

## The API

This section only serves as a brief demonstration of the core API. For full runnable examples, see the [`examples` directory](./examples).

> [!TIP]
>
> The macros shown below (`query!`, `insert!`, `update!`, `delete!`) are only thin wrappers that define relevant types and applying derive macros.
>
> It's always possible to manually define the relevant model types first and use the derived methods. See the [model-first example](./examples/pg_query_model_first.rs).

### Schema DSL

Define your schema with a DSL that feels familiar if you have used Prisma. Schema validation happens at compile time:

```rust
schema! {
  name app_schema

  model user {
    id         Int      @id @default(autoincrement())
    email      String   @unique
    name       String   @index
    created_at DateTime @default(now())
    posts      post[]
  }

  model post {
    id         Int      @id @default(autoincrement())
    title      String
    published  Boolean  @default(false)
    author_id  Int
    author     user     @relation(fields: [author_id], references: [id])
    comments   comment[]

    @@index([author_id, published])
  }

  model comment {
    id      Int    @id @default(autoincrement())
    body    String
    post_id Int
    post    post   @relation(fields: [post_id], references: [id])
  }
}
```

### Nested queries

Selections, includes, filters, ordering, and pagination follow the relation graph directly:

```rust
query! {
  crate::app_schema,
  user {
    select: {
      id: true,
      email: true,
      name: true,
    },
    include: {
      posts: {
        select: {
          id: true,
          title: true,
          published: true,
        },
        include: {
          comments: {
            select: {
              body: true,
            },
            order_by: [
              { body: desc },
            ],
            limit: 3,
          },
        },
        where: {
          published: {
            eq: true,
          },
        },
        order_by: [
          { title: desc },
        ],
        skip: 5,
        limit: 10,
      },
    },
    order_by: [
      { created_at: desc },
    ],
    limit: 20,
  }
}
```

Again, query validation happens at compile time. The statement returns typed data:

```rust
let users = client.find_many(query! { ... }).await.unwrap();

// Everything in the entity fetched is typed
let latest_post_title: &str = &users[0].posts[0].title;
println!("Latest post on this page: {latest_post_title}");
```

### Writes

Reads and writes share the same vocabulary, so inserts, updates, and deletes feel consistent with queries:

```rust
insert! {
  crate::app_schema,
  post {
    data: {
      title: "Hello Vitrail".to_owned(),
      published: false,
      author_id: author.id,
    },
  }
}
```

```rust
update! {
  crate::app_schema,
  post {
    data: {
      published: true,
    },
    where: {
      author: {
        email: {
          eq: "alice@example.com".to_owned(),
        },
      },
    },
  }
}
```

```rust
delete! {
  crate::app_schema,
  comment {
    where: {
      post: {
        author: {
          email: {
            eq: "alice@example.com".to_owned(),
          },
        },
      },
    },
  }
}
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](./LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
