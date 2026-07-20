pub use vitrail_d1_core::*;
pub use vitrail_d1_macros::{
    DeleteMany, InsertInput, InsertResult, QueryResult, QueryVariables, UpdateData, UpdateMany,
    delete, insert, query, schema, update,
};
extern crate self as vitrail_d1;

schema! {
    name operation_schema

    model user {
        id     Int     @id @default(autoincrement())
        email  String  @unique
        name   String
        active Boolean
    }
}

#[derive(QueryVariables)]
struct UserIdVariables {
    user_id: i64,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::operation_schema::Schema,
    model = user,
    variables = UserIdVariables,
    where(id = eq(user_id))
)]
struct UserById {
    id: i64,
    email: String,
    name: String,
    active: bool,
}

#[derive(InsertInput)]
#[vitrail(schema = crate::operation_schema::Schema, model = user)]
struct NewUser {
    email: String,
    name: String,
    active: bool,
}

#[derive(InsertResult)]
#[vitrail(
    schema = crate::operation_schema::Schema,
    model = user,
    input = NewUser
)]
struct InsertedUser {
    id: i64,
    email: String,
    name: String,
    active: bool,
}

#[derive(UpdateData)]
#[vitrail(schema = crate::operation_schema::Schema, model = user)]
struct RenameUser {
    name: String,
}

#[derive(UpdateMany)]
#[vitrail(
    schema = crate::operation_schema::Schema,
    model = user,
    data = RenameUser,
    variables = UserIdVariables,
    where(id = eq(user_id))
)]
struct RenameUserById;

#[derive(DeleteMany)]
#[vitrail(
    schema = crate::operation_schema::Schema,
    model = user,
    variables = UserIdVariables,
    where(id = eq(user_id))
)]
struct DeleteUserById;

fn main() {
    let _ = crate::operation_schema::query_with_variables::<UserById>(
        UserIdVariables { user_id: 1 },
    );

    let _ = query! {
        crate::operation_schema,
        user {
            select: {
                id: true,
                email: true,
            },
            where: {
                id: {
                    eq: 1_i64,
                },
            },
        }
    };

    let _ = crate::operation_schema::insert::<InsertedUser>(NewUser {
        email: "alice@example.com".to_owned(),
        name: "Alice".to_owned(),
        active: true,
    });

    let _ = insert! {
        crate::operation_schema,
        user {
            data: {
                email: "bob@example.com".to_owned(),
                name: "Bob".to_owned(),
                active: true,
            },
        }
    };

    let _ = crate::operation_schema::update_many_with_variables::<RenameUserById>(
        UserIdVariables { user_id: 1 },
        RenameUser {
            name: "Alicia".to_owned(),
        },
    );

    let _ = update! {
        crate::operation_schema,
        user {
            data: {
                name: "Updated".to_owned(),
            },
            where: {
                id: {
                    eq: 1_i64,
                },
            },
        }
    };

    let _ = crate::operation_schema::delete_many_with_variables::<DeleteUserById>(
        UserIdVariables { user_id: 1 },
    );

    let _ = delete! {
        crate::operation_schema,
        user {
            where: {
                id: {
                    eq: 1_i64,
                },
            },
        }
    };
}
