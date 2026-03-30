use vitrail_pg::{
    DeleteMany, InsertInput, InsertResult, QueryResult, QueryVariables, UpdateData, UpdateMany,
    VitrailClient, schema, uuid::Uuid,
};

schema! {
    name my_schema

    model user {
        id          Int      @id @default(autoincrement())
        external_id String   @unique @db.Uuid
        email       String   @unique
        name        String
        created_at  DateTime @default(now())
        posts       post[]
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

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(schema = crate::my_schema::Schema, model = post)]
struct PostSummary {
    id: i64,
    title: String,
}

#[allow(dead_code)]
#[derive(InsertInput)]
#[vitrail(schema = crate::my_schema::Schema, model = user)]
struct NewUser {
    external_id: Uuid,
    email: String,
    name: String,
}

#[allow(dead_code)]
#[derive(InsertResult)]
#[vitrail(schema = crate::my_schema::Schema, model = user, input = NewUser)]
struct InsertedUser {
    id: i64,
    external_id: Uuid,
    email: String,
    name: String,
}

#[allow(dead_code)]
#[derive(InsertInput)]
#[vitrail(schema = crate::my_schema::Schema, model = post)]
struct NewPost {
    title: String,
    body: Option<String>,
    published: bool,
    author_id: i64,
}

#[allow(dead_code)]
#[derive(InsertResult)]
#[vitrail(schema = crate::my_schema::Schema, model = post, input = NewPost)]
struct InsertedPost {
    id: i64,
    title: String,
    published: bool,
    author_id: i64,
}

#[derive(QueryVariables)]
struct UserByIdVariables {
    user_id: i64,
}

#[derive(QueryVariables)]
struct PostsByAuthorEmailVariables {
    author_email: String,
}

#[derive(QueryVariables)]
struct PostByExcludedTitleVariables {
    excluded_title: String,
}

#[allow(dead_code)]
#[derive(UpdateData)]
#[vitrail(schema = crate::my_schema::Schema, model = post)]
struct PublishPostsData {
    published: bool,
}

#[allow(dead_code)]
#[derive(UpdateMany)]
#[vitrail(
    schema = crate::my_schema::Schema,
    model = post,
    data = PublishPostsData,
    variables = PostsByAuthorEmailVariables,
    where(author.email = eq(author_email))
)]
struct PublishPostsByAuthorEmail;

#[allow(dead_code)]
#[derive(DeleteMany)]
#[vitrail(
    schema = crate::my_schema::Schema,
    model = post,
    variables = PostByExcludedTitleVariables,
    where(title = not(excluded_title))
)]
struct DeletePostsByExcludedTitle;

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(
    schema = crate::my_schema::Schema,
    model = user,
    variables = UserByIdVariables,
    where(id = eq(user_id))
)]
struct UserWithPosts {
    id: i64,
    external_id: Uuid,
    email: String,
    name: String,
    #[vitrail(include)]
    posts: Vec<PostSummary>,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(
    schema = crate::my_schema::Schema,
    model = post,
    variables = PostByExcludedTitleVariables,
    where(title = not(excluded_title))
)]
struct PostWithDifferentTitle {
    id: i64,
    title: String,
}

#[tokio::main]
async fn main() {
    let client = VitrailClient::new("postgres://postgres:postgres@127.0.0.1:5432/vitrail")
        .await
        .unwrap();
    let external_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();

    let user = client
        .insert(my_schema::insert::<InsertedUser>(NewUser {
            external_id,
            email: "alice@example.com".to_owned(),
            name: "Alice".to_owned(),
        }))
        .await
        .unwrap();

    client
        .insert(my_schema::insert::<InsertedPost>(NewPost {
            title: "Hello Vitrail".to_owned(),
            body: Some("Draft body".to_owned()),
            published: false,
            author_id: user.id,
        }))
        .await
        .unwrap();

    client
        .insert(my_schema::insert::<InsertedPost>(NewPost {
            title: "Untitled draft".to_owned(),
            body: None,
            published: false,
            author_id: user.id,
        }))
        .await
        .unwrap();

    let updated_posts = client
        .update_many(my_schema::update_many_with_variables::<
            PublishPostsByAuthorEmail,
        >(
            PostsByAuthorEmailVariables {
                author_email: "alice@example.com".to_owned(),
            },
            PublishPostsData { published: true },
        ))
        .await
        .unwrap();

    let deleted_posts = client
        .delete_many(my_schema::delete_many_with_variables::<
            DeletePostsByExcludedTitle,
        >(PostByExcludedTitleVariables {
            excluded_title: "Hello Vitrail".to_owned(),
        }))
        .await
        .unwrap();

    let users = client
        .find_many(my_schema::query_with_variables::<UserWithPosts>(
            UserByIdVariables { user_id: user.id },
        ))
        .await
        .unwrap();

    let posts = client
        .find_many(my_schema::query_with_variables::<PostWithDifferentTitle>(
            PostByExcludedTitleVariables {
                excluded_title: "Untitled draft".to_owned(),
            },
        ))
        .await
        .unwrap();

    println!("inserted user {} ({})", user.email, user.external_id);
    println!("updated {} posts", updated_posts);
    println!("deleted {} posts", deleted_posts);
    println!("fetched {} users", users.len());
    println!("fetched {} posts with a not(...) filter", posts.len());
}
