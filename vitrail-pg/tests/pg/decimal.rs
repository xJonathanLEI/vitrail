use std::str::FromStr;

use crate::support::{TestDatabase, apply_schema};
use sqlx::postgres::PgPoolOptions;
use vitrail_pg::{
    PostgresSchema, QueryResult, QueryVariables, UpdateData, UpdateMany, VitrailClient, insert,
    query, rust_decimal::Decimal, schema,
};

schema! {
    name decimal_schema

    model invoice {
        id        Int      @id @default(autoincrement())
        reference String   @unique
        total     Decimal
        tax       Decimal?
        lines     invoice_line[]
    }

    model invoice_line {
        id          Int     @id @default(autoincrement())
        description String
        amount      Decimal
        invoice_id  Int
        invoice     invoice @relation(fields: [invoice_id], references: [id])
    }
}

pub(crate) use self::decimal_schema as pg_decimal_schema;

#[derive(QueryVariables)]
struct InvoiceByTotalVariables {
    total: Decimal,
}

#[derive(QueryResult)]
#[vitrail(schema = crate::decimal_schema::Schema, model = invoice_line)]
struct InvoiceLineSummary {
    id: i64,
    description: String,
    amount: Decimal,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::decimal_schema::Schema,
    model = invoice,
    variables = InvoiceByTotalVariables,
    where(total = eq(total))
)]
struct InvoiceByTotal {
    id: i64,
    reference: String,
    total: Decimal,
    tax: Option<Decimal>,
    #[vitrail(include)]
    lines: Vec<InvoiceLineSummary>,
}

#[derive(UpdateData)]
#[vitrail(schema = crate::decimal_schema::Schema, model = invoice)]
struct UpdateInvoiceAmountsData {
    total: Decimal,
    tax: Option<Decimal>,
}

#[derive(UpdateMany)]
#[vitrail(
    schema = crate::decimal_schema::Schema,
    model = invoice,
    data = UpdateInvoiceAmountsData,
    variables = InvoiceByTotalVariables,
    where(total = eq(total))
)]
struct UpdateInvoiceAmountsByTotal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).expect("decimal literal should parse")
}

#[test]
fn decimal_columns_generate_decimal_migration_sql() {
    let sql = PostgresSchema::from_schema_access::<crate::decimal_schema::Schema>()
        .migrate_from(&PostgresSchema::empty())
        .to_sql();

    assert!(
        sql.contains("\"total\" DECIMAL(65,30) NOT NULL"),
        "invoice total should migrate to DECIMAL(65,30), got:\n{sql}"
    );
    assert!(
        sql.contains("\"tax\" DECIMAL(65,30)"),
        "optional invoice tax should migrate to DECIMAL(65,30), got:\n{sql}"
    );
    assert!(
        sql.contains("\"amount\" DECIMAL(65,30) NOT NULL"),
        "invoice line amount should migrate to DECIMAL(65,30), got:\n{sql}"
    );
}

#[tokio::test]
async fn decimal_migrations_roundtrip_through_postgres_introspection() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    let target = PostgresSchema::from_schema_access::<crate::decimal_schema::Schema>();
    let migration = target.migrate_from(&PostgresSchema::empty());

    apply_schema(&database_url, &target).await;

    let introspected = PostgresSchema::introspect(&database_url)
        .await
        .expect("should introspect postgres schema with decimal columns");
    let second_pass = target.migrate_from(&introspected);

    assert_eq!(
        migration.to_sql(),
        target.migrate_from(&PostgresSchema::empty()).to_sql()
    );
    assert!(
        second_pass.is_empty(),
        "decimal schema should roundtrip without pending migration, got:\n{}",
        second_pass.to_sql()
    );

    database.cleanup().await;
}

#[tokio::test]
async fn decimals_roundtrip_across_insert_query_update_include_and_sqlx() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    apply_schema(
        &database_url,
        &PostgresSchema::from_schema_access::<crate::decimal_schema::Schema>(),
    )
    .await;

    let original_total = decimal("1234567890.123456789123456789");
    let original_tax = decimal("0.000000000000000123");
    let updated_total = decimal("2222222222.222222222222222222");
    let line_amount = decimal("999999999.000000001");

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let invoice = client
        .insert(insert! {
            crate::decimal_schema,
            invoice {
                data: {
                    reference: "INV-001".to_owned(),
                    total: original_total,
                    tax: Some(original_tax),
                },
                select: {
                    id: true,
                    reference: true,
                    total: true,
                    tax: true,
                },
            }
        })
        .await
        .expect("invoice insert should succeed");

    assert!(invoice.id > 0);
    assert_eq!(invoice.reference, "INV-001");
    assert_eq!(invoice.total, original_total);
    assert_eq!(invoice.tax, Some(original_tax));

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let inserted_line_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO "invoice_line" ("description", "amount", "invoice_id")
        VALUES ($1, $2, $3)
        RETURNING "id"::bigint
        "#,
    )
    .bind("Precision-critical line")
    .bind(line_amount)
    .bind(invoice.id)
    .fetch_one(&pool)
    .await
    .expect("invoice line insert should succeed");

    let queried_invoices = client
        .find_many(
            crate::decimal_schema::query_with_variables::<InvoiceByTotal>(
                InvoiceByTotalVariables {
                    total: original_total,
                },
            ),
        )
        .await
        .expect("decimal query should succeed");

    assert_eq!(queried_invoices.len(), 1);
    let queried_invoice = &queried_invoices[0];
    assert_eq!(queried_invoice.id, invoice.id);
    assert_eq!(queried_invoice.reference, "INV-001");
    assert_eq!(queried_invoice.total, original_total);
    assert_eq!(queried_invoice.tax, Some(original_tax));
    assert_eq!(queried_invoice.lines.len(), 1);
    assert_eq!(queried_invoice.lines[0].id, inserted_line_id);
    assert_eq!(
        queried_invoice.lines[0].description,
        "Precision-critical line"
    );
    assert_eq!(queried_invoice.lines[0].amount, line_amount);

    let updated_count = client
        .update_many(crate::decimal_schema::update_many_with_variables::<
            UpdateInvoiceAmountsByTotal,
        >(
            InvoiceByTotalVariables {
                total: original_total,
            },
            UpdateInvoiceAmountsData {
                total: updated_total,
                tax: None,
            },
        ))
        .await
        .expect("decimal update should succeed");

    assert_eq!(updated_count, 1);

    let invoices_with_lines = client
        .find_many(query! {
            crate::decimal_schema,
            invoice {
                select: {
                    id: true,
                    reference: true,
                    total: true,
                    tax: true,
                },
                include: {
                    lines: {
                        select: {
                            id: true,
                            description: true,
                            amount: true,
                        },
                    },
                },
            }
        })
        .await
        .expect("query helper should decode decimal includes");

    assert_eq!(invoices_with_lines.len(), 1);
    assert_eq!(invoices_with_lines[0].total, updated_total);
    assert_eq!(invoices_with_lines[0].tax, None);
    assert_eq!(invoices_with_lines[0].lines.len(), 1);
    assert_eq!(invoices_with_lines[0].lines[0].amount, line_amount);

    let stored = sqlx::query_as::<_, (String, Decimal, Option<Decimal>, Decimal)>(
        r#"
        SELECT i."reference", i."total", i."tax", l."amount"
        FROM "invoice" AS i
        JOIN "invoice_line" AS l ON l."invoice_id" = i."id"
        WHERE i."id" = $1
        "#,
    )
    .bind(invoice.id)
    .fetch_one(&pool)
    .await
    .expect("should fetch stored decimal values");

    assert_eq!(stored.0, "INV-001");
    assert_eq!(stored.1, updated_total);
    assert_eq!(stored.2, None);
    assert_eq!(stored.3, line_amount);

    pool.close().await;
    client.close().await;
    database.cleanup().await;
}
