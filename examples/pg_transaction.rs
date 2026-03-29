use vitrail_pg::{
    TransactionIsolationLevel, TransactionOptions, VitrailClient, VitrailTransaction, insert,
    query, schema,
};

schema! {
    name my_schema

    model user {
        id         Int      @id @default(autoincrement())
        email      String   @unique
        name       String
        created_at DateTime @default(now())
        posts      post[]
    }

    model post {
        id         Int      @id @default(autoincrement())
        title      String
        body       String?
        published  Boolean
        author_id  Int
        created_at DateTime @default(now())
        author     user     @relation(fields: [author_id], references: [id])
    }
}

async fn create_user(
    txn: &VitrailTransaction,
    email: &str,
    name: &str,
) -> Result<i64, sqlx::Error> {
    let user = txn
        .insert(insert! {
            crate::my_schema,
            user {
                data: {
                    email: email.to_owned(),
                    name: name.to_owned(),
                },
            }
        })
        .await?;

    Ok(user.id)
}

async fn create_draft_post(
    txn: &VitrailTransaction,
    author_id: i64,
    title: &str,
) -> Result<(), sqlx::Error> {
    txn.insert(insert! {
        crate::my_schema,
        post {
            data: {
                title: title.to_owned(),
                body: Some("Created inside an explicit transaction".to_owned()),
                published: false,
                author_id: author_id,
            },
        }
    })
    .await?;

    Ok(())
}

async fn create_user_with_welcome_post(
    txn: &VitrailTransaction,
    email: &str,
    name: &str,
) -> Result<i64, sqlx::Error> {
    let user_id = create_user(txn, email, name).await?;
    create_draft_post(txn, user_id, "Welcome to Vitrail").await?;
    Ok(user_id)
}

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let client = VitrailClient::new("postgres://postgres:postgres@127.0.0.1:5432/vitrail").await?;

    let txn = client
        .begin_with_options(TransactionOptions::with_isolation_level(
            TransactionIsolationLevel::Serializable,
        ))
        .await?;

    let user_id = create_user_with_welcome_post(&txn, "alice@example.com", "Alice").await?;

    let user = txn
        .find_first(query! {
            crate::my_schema,
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
                    },
                },
                where: {
                    id: {
                        eq: user_id
                    },
                },
            }
        })
        .await?;

    println!(
        "created user {} with {} post(s) inside the transaction",
        user.email,
        user.posts.len()
    );

    txn.commit().await?;

    let persisted = client
        .find_first(query! {
            crate::my_schema,
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
                    },
                },
                where: {
                    id: {
                        eq: user_id
                    },
                },
            }
        })
        .await?;

    println!(
        "after commit, user {} still has {} post(s)",
        persisted.email,
        persisted.posts.len()
    );

    Ok(())
}
