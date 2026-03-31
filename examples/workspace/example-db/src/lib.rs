use vitrail_pg::{StringValueType, schema};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostalCode(String);

impl PostalCode {
    pub fn parse(value: impl Into<String>) -> Result<Self, PostalCodeError> {
        let value = value.into();
        let is_valid = value.len() == 5 && value.chars().all(|ch| ch.is_ascii_digit());

        if is_valid {
            Ok(Self(value))
        } else {
            Err(PostalCodeError(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug)]
pub struct PostalCodeError(String);

impl std::fmt::Display for PostalCodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid postal code `{}`", self.0)
    }
}

impl std::error::Error for PostalCodeError {}

impl StringValueType for PostalCode {
    fn from_db_string(value: String) -> Result<Self, vitrail_pg::sqlx::Error> {
        PostalCode::parse(value).map_err(|error| vitrail_pg::sqlx::Error::Decode(Box::new(error)))
    }

    fn into_db_string(self) -> String {
        self.0
    }
}

schema! {
    name app_schema

    model user {
        id          Int      @id @default(autoincrement())
        email       String   @unique
        name        String
        created_at  DateTime @default(now())
        addresses   address[]
        posts       post[]
    }

    model address {
        id          Int     @id @default(autoincrement())
        postal_code String  @rust_ty(crate::PostalCode)
        user_id     Int
        user        user    @relation(fields: [user_id], references: [id])
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
