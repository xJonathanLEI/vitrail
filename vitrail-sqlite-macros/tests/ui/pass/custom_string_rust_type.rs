pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::schema;
extern crate self as vitrail_sqlite;

#[derive(Clone, Debug, Eq, PartialEq)]
struct PostalCode(String);

schema! {
    name custom_string_rust_type_schema

    model address {
        id          Int    @id @default(autoincrement())
        postal_code String @rust_ty(crate::PostalCode)
    }
}

fn main() {}
