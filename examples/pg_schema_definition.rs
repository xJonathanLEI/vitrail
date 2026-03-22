use sqlx::{Row as _, postgres::PgRow};
use vitrail_pg::{QueryModel, QueryRelationSelection, QuerySelection, alias_name, schema};

schema! {
    name my_schema

    model user {
        id         Int      @id @default(autoincrement())
        email      String   @unique
        name       String
        created_at DateTime @default(now())
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

struct QueryUser {
    id: i64,
    email: String,
    name: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl QueryModel for QueryUser {
    type Schema = my_schema::Schema;

    fn model_name() -> &'static str {
        "user"
    }

    fn selection() -> QuerySelection {
        QuerySelection {
            model: Self::model_name(),
            scalar_fields: vec!["id", "email", "name", "created_at"],
            relations: Vec::new(),
        }
    }

    fn from_row(row: &PgRow, prefix: &str) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get(alias_name(prefix, "id").as_str())?,
            email: row.try_get(alias_name(prefix, "email").as_str())?,
            name: row.try_get(alias_name(prefix, "name").as_str())?,
            created_at: row.try_get(alias_name(prefix, "created_at").as_str())?,
        })
    }
}

struct QueryPost {
    id: i64,
    title: String,
    author: QueryUser,
}

impl QueryModel for QueryPost {
    type Schema = my_schema::Schema;

    fn model_name() -> &'static str {
        "post"
    }

    fn selection() -> QuerySelection {
        QuerySelection {
            model: Self::model_name(),
            scalar_fields: vec!["id", "title"],
            relations: vec![QueryRelationSelection {
                field: "author",
                selection: QueryUser::selection(),
            }],
        }
    }

    fn from_row(row: &PgRow, prefix: &str) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get(alias_name(prefix, "id").as_str())?,
            title: row.try_get(alias_name(prefix, "title").as_str())?,
            author: QueryUser::from_row(row, &alias_name(prefix, "author"))?,
        })
    }
}

#[tokio::main]
async fn main() {
    let client = my_schema::VitrailClient::new("postgres://127.0.0.1:5432/vitrail")
        .await
        .unwrap();

    let posts = client
        .find_many(my_schema::query::<QueryPost>())
        .await
        .unwrap();
    let post = &posts[0];

    println!(
        "Post #{}: {} (#{} {} [{}]; joined {})",
        post.id,
        post.title,
        post.author.id,
        post.author.name,
        post.author.email,
        post.author.created_at
    );
}
