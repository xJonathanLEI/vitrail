use serde_json::json;
use vitrail_d1::{Error as VitrailError, VitrailClient, query, schema};
use worker::{
    Context, Env, Error as WorkerError, Request, Response, Result as WorkerResult, event,
};

schema! {
    name todo_schema

    model todo {
        id        Int     @id @default(autoincrement())
        title     String
        completed Boolean
    }
}

#[event(fetch)]
pub async fn fetch(_request: Request, env: Env, _context: Context) -> WorkerResult<Response> {
    let client = VitrailClient::new(env.d1("DB")?);
    let todos = client
        .find_many(query! {
            crate::todo_schema,
            todo {
                select: {
                    id: true,
                    title: true,
                    completed: true,
                },
                order_by: [
                    { id: asc },
                ],
            }
        })
        .await
        .map_err(worker_error)?;

    let response = todos
        .into_iter()
        .map(|todo| {
            json!({
                "id": todo.id.to_string(),
                "title": todo.title,
                "completed": todo.completed,
            })
        })
        .collect::<Vec<_>>();

    Response::from_json(&response)
}

fn worker_error(error: VitrailError) -> WorkerError {
    WorkerError::RustError(error.to_string())
}
