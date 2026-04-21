use crate::support::{TestDatabase, apply_schema};
use sqlx::postgres::PgPoolOptions;
use vitrail_pg::{
    PostgresSchema, QueryResult, QueryVariables, UpdateData, UpdateMany, VitrailClient, insert,
    query, schema,
};

schema! {
    name bigint_schema

    model account {
        id           BigInt   @id @default(autoincrement())
        external_ref BigInt   @unique
        credit_limit BigInt
        invoices     invoice[]
    }

    model invoice {
        id           BigInt  @id @default(autoincrement())
        account_id   BigInt  @index
        amount_cents BigInt
        settled_at   BigInt?
        account      account @relation(fields: [account_id], references: [id])

        @@unique([account_id, amount_cents])
    }
}

pub(crate) use self::bigint_schema as pg_bigint_schema;

#[derive(QueryVariables)]
struct AccountByExternalRefVariables {
    external_ref: i64,
}

#[derive(QueryVariables)]
struct InvoiceByAccountAndAmountVariables {
    old_amount_cents: i64,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(schema = crate::bigint_schema::Schema, model = invoice, order_by(amount_cents = desc))]
struct InvoiceSummary {
    id: i64,
    amount_cents: i64,
    settled_at: Option<i64>,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(
    schema = crate::bigint_schema::Schema,
    model = account,
    variables = AccountByExternalRefVariables,
    where(external_ref = eq(external_ref))
)]
struct AccountWithInvoices {
    id: i64,
    external_ref: i64,
    credit_limit: i64,
    #[vitrail(include)]
    invoices: Vec<InvoiceSummary>,
}

#[derive(UpdateData)]
#[vitrail(schema = crate::bigint_schema::Schema, model = invoice)]
struct UpdateInvoiceBigIntData {
    amount_cents: i64,
    settled_at: Option<i64>,
}

#[derive(UpdateMany)]
#[vitrail(
    schema = crate::bigint_schema::Schema,
    model = invoice,
    data = UpdateInvoiceBigIntData,
    variables = InvoiceByAccountAndAmountVariables,
    where(amount_cents = eq(old_amount_cents))
)]
struct UpdateInvoiceByAccountAndAmount;

#[test]
fn bigint_columns_generate_bigint_migration_sql() {
    let sql = PostgresSchema::from_schema_access::<crate::bigint_schema::Schema>()
        .migrate_from(&PostgresSchema::empty())
        .to_sql();

    assert!(sql.contains(r#""id" BIGSERIAL NOT NULL"#));
    assert!(sql.contains(r#""external_ref" BIGINT NOT NULL"#));
    assert!(sql.contains(r#""credit_limit" BIGINT NOT NULL"#));
    assert!(sql.contains(r#""account_id" BIGINT NOT NULL"#));
    assert!(sql.contains(r#""amount_cents" BIGINT NOT NULL"#));
    assert!(sql.contains(r#""settled_at" BIGINT"#));
}

#[tokio::test]
async fn bigint_columns_work_across_relations_query_and_update() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    apply_schema(
        &database_url,
        &PostgresSchema::from_schema_access::<crate::bigint_schema::Schema>(),
    )
    .await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let account = client
        .insert(insert! {
            crate::bigint_schema,
            account {
                data: {
                    external_ref: 9_223_372_036_854_i64,
                    credit_limit: 5_000_000_000_i64,
                },
                select: {
                    id: true,
                    external_ref: true,
                    credit_limit: true,
                },
            }
        })
        .await
        .expect("bigint account insert should succeed");

    assert!(account.id > 0);
    assert_eq!(account.external_ref, 9_223_372_036_854_i64);
    assert_eq!(account.credit_limit, 5_000_000_000_i64);

    let first_invoice = client
        .insert(insert! {
            crate::bigint_schema,
            invoice {
                data: {
                    account_id: account.id,
                    amount_cents: 1_999_999_999_i64,
                    settled_at: None::<i64>,
                },
                select: {
                    id: true,
                    account_id: true,
                    amount_cents: true,
                    settled_at: true,
                },
            }
        })
        .await
        .expect("first bigint invoice insert should succeed");

    let second_invoice = client
        .insert(insert! {
            crate::bigint_schema,
            invoice {
                data: {
                    account_id: account.id,
                    amount_cents: 2_599_999_999_i64,
                    settled_at: Some(1_700_000_000_123_i64),
                },
                select: {
                    id: true,
                    account_id: true,
                    amount_cents: true,
                    settled_at: true,
                },
            }
        })
        .await
        .expect("second bigint invoice insert should succeed");

    assert!(first_invoice.id > 0);
    assert!(second_invoice.id > first_invoice.id);
    assert_eq!(first_invoice.account_id, account.id);
    assert_eq!(second_invoice.account_id, account.id);
    assert_eq!(first_invoice.amount_cents, 1_999_999_999_i64);
    assert_eq!(second_invoice.amount_cents, 2_599_999_999_i64);
    assert_eq!(first_invoice.settled_at, None);
    assert_eq!(second_invoice.settled_at, Some(1_700_000_000_123_i64));

    let accounts = client
        .find_many(crate::bigint_schema::query_with_variables::<
            AccountWithInvoices,
        >(AccountByExternalRefVariables {
            external_ref: account.external_ref,
        }))
        .await
        .expect("bigint relation query should succeed");

    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].id, account.id);
    assert_eq!(accounts[0].external_ref, account.external_ref);
    assert_eq!(accounts[0].credit_limit, account.credit_limit);
    assert_eq!(accounts[0].invoices.len(), 2);
    assert_eq!(accounts[0].invoices[0].amount_cents, 2_599_999_999_i64);
    assert_eq!(accounts[0].invoices[1].amount_cents, 1_999_999_999_i64);

    let queried_invoices = client
        .find_many(query! {
            crate::bigint_schema,
            invoice {
                select: {
                    id: true,
                    amount_cents: true,
                    settled_at: true,
                },
                include: {
                    account: {
                        select: {
                            id: true,
                            external_ref: true,
                            credit_limit: true,
                        },
                    },
                },
                where: {
                    amount_cents: {
                        in: vec![1_999_999_999_i64, 2_599_999_999_i64]
                    },
                },
                order_by: [
                    { amount_cents: desc },
                ],
            }
        })
        .await
        .expect("bigint in-filter query should succeed");

    assert_eq!(queried_invoices.len(), 2);
    assert_eq!(queried_invoices[0].amount_cents, 2_599_999_999_i64);
    assert_eq!(
        queried_invoices[0].account.external_ref,
        account.external_ref
    );
    assert_eq!(queried_invoices[1].amount_cents, 1_999_999_999_i64);
    assert_eq!(
        queried_invoices[1].account.credit_limit,
        account.credit_limit
    );

    let updated_count = client
        .update_many(crate::bigint_schema::update_many_with_variables::<
            UpdateInvoiceByAccountAndAmount,
        >(
            InvoiceByAccountAndAmountVariables {
                old_amount_cents: first_invoice.amount_cents,
            },
            UpdateInvoiceBigIntData {
                amount_cents: 4_999_999_999_i64,
                settled_at: Some(1_700_000_000_999_i64),
            },
        ))
        .await
        .expect("bigint update should succeed");

    assert_eq!(updated_count, 1);

    let stored = sqlx::query_as::<_, (i64, i64, Option<i64>)>(
        r#"
        SELECT "account_id", "amount_cents", "settled_at"
        FROM "invoice"
        WHERE "id" = $1
        "#,
    )
    .bind(first_invoice.id)
    .fetch_one(&pool)
    .await
    .expect("should read updated bigint invoice directly from postgres");

    assert_eq!(stored.0, account.id);
    assert_eq!(stored.1, 4_999_999_999_i64);
    assert_eq!(stored.2, Some(1_700_000_000_999_i64));

    let accounts_after_update = client
        .find_many(crate::bigint_schema::query_with_variables::<
            AccountWithInvoices,
        >(AccountByExternalRefVariables {
            external_ref: account.external_ref,
        }))
        .await
        .expect("bigint relation query after update should succeed");

    assert_eq!(accounts_after_update.len(), 1);
    assert_eq!(accounts_after_update[0].invoices.len(), 2);
    assert_eq!(
        accounts_after_update[0].invoices[0].amount_cents,
        4_999_999_999_i64
    );
    assert_eq!(
        accounts_after_update[0].invoices[0].settled_at,
        Some(1_700_000_000_999_i64)
    );
    assert_eq!(
        accounts_after_update[0].invoices[1].amount_cents,
        2_599_999_999_i64
    );

    pool.close().await;
    client.close().await;
    database.cleanup().await;
}
