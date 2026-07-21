use std::any::Any;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::Deserialize;
use vitrail_sqlite_dialect::CompiledStatement;
use worker::d1::{D1Database, D1DatabaseSession, D1PreparedStatement, D1Result};
use worker::wasm_bindgen::JsValue;

use crate::row::D1RowMetadata;
use crate::statement::{changes_from_result, prepare_statement};
use crate::{D1Executor, D1Row, DeleteSpec, Error, InsertSpec, QuerySpec, UpdateSpec};

type BoxBatchValue = Box<dyn Any + Send>;
type BatchDecoder =
    Box<dyn FnOnce(&CompiledStatement, D1Result) -> Result<BoxBatchValue, Error> + Send + 'static>;

/// Internal adapter from queued batch operations to their resolved outputs.
///
/// This trait is sealed and exists only as the generic bound used by the
/// high-level atomic-batch convenience API.
#[doc(hidden)]
pub trait BatchOutput: private::Sealed {
    /// The concrete value returned after the atomic batch executes.
    type Output;

    #[doc(hidden)]
    type Handles: Send + 'static;

    #[doc(hidden)]
    fn into_handles(self) -> Result<Self::Handles, Error>;

    #[doc(hidden)]
    fn extract(handles: Self::Handles, results: &mut BatchResults) -> Result<Self::Output, Error>;
}

mod private {
    pub trait Sealed {}
}

#[derive(Debug)]
struct BatchIdentity;

#[derive(Clone, Copy)]
enum AtomicBatchTarget<'a> {
    Database(&'a D1Database),
    Session(&'a D1DatabaseSession),
}

impl AtomicBatchTarget<'_> {
    fn executor(&self) -> &dyn D1Executor {
        match self {
            Self::Database(database) => *database,
            Self::Session(session) => *session,
        }
    }

    async fn execute(self, statements: Vec<D1PreparedStatement>) -> Result<Vec<D1Result>, Error> {
        match self {
            Self::Database(database) => database.batch(statements).await.map_err(Error::from),
            Self::Session(session) => session.batch(statements).await.map_err(Error::from),
        }
    }
}

struct BatchEntry {
    statement: CompiledStatement,
    generation: u64,
    decoder: BatchDecoder,
}

/// A typed Cloudflare D1 atomic batch.
///
/// Operations are compiled as they are added. Calling [`AtomicBatch::execute`]
/// prepares and binds every statement before submitting exactly one D1
/// `batch()` call. D1 executes the submitted statements atomically and in order.
///
/// This API represents D1's atomic multi-statement primitive. It is not an
/// interactive transaction and does not expose callback or incremental
/// execution semantics.
pub struct AtomicBatch<'a> {
    target: AtomicBatchTarget<'a>,
    identity: Arc<BatchIdentity>,
    entries: Vec<BatchEntry>,
}

impl fmt::Debug for AtomicBatch<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AtomicBatch")
            .field("operation_count", &self.entries.len())
            .finish_non_exhaustive()
    }
}

impl<'a> AtomicBatch<'a> {
    pub(crate) fn for_database(database: &'a D1Database) -> Self {
        Self::new(AtomicBatchTarget::Database(database))
    }

    pub(crate) fn for_session(session: &'a D1DatabaseSession) -> Self {
        Self::new(AtomicBatchTarget::Session(session))
    }

    fn new(target: AtomicBatchTarget<'a>) -> Self {
        Self {
            target,
            identity: Arc::new(BatchIdentity),
            entries: Vec::new(),
        }
    }

    /// Returns the number of operations currently queued in this batch.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` when the batch contains no queued operations.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Queues a query that returns every matching row.
    pub fn find_many<Q>(&mut self, query: Q) -> Result<BatchHandle<Vec<Q::Output>>, Error>
    where
        Q: QuerySpec + 'static,
    {
        let statement = query.compile_batch_many()?;
        let metadata = validated_row_metadata(&statement)?;

        Ok(self.push(statement, move |statement, result| {
            let rows = rows_from_result(result, statement, metadata)?;
            let output = query.decode_batch_rows(rows)?;
            Ok(Box::new(output))
        }))
    }

    /// Queues a query that returns the first matching row, if one exists.
    pub fn find_optional<Q>(&mut self, query: Q) -> Result<BatchHandle<Option<Q::Output>>, Error>
    where
        Q: QuerySpec + 'static,
    {
        let statement = query.compile_batch_single()?;
        let metadata = validated_row_metadata(&statement)?;

        Ok(self.push(statement, move |statement, result| {
            let rows = rows_from_result(result, statement, metadata)?;
            let output = query.decode_batch_rows(rows)?.into_iter().next();
            Ok(Box::new(output))
        }))
    }

    /// Queues a query that requires one matching row.
    pub fn find_first<Q>(&mut self, query: Q) -> Result<BatchHandle<Q::Output>, Error>
    where
        Q: QuerySpec + 'static,
    {
        let statement = query.compile_batch_single()?;
        let metadata = validated_row_metadata(&statement)?;

        Ok(self.push(statement, move |statement, result| {
            let rows = rows_from_result(result, statement, metadata)?;
            let output = query
                .decode_batch_rows(rows)?
                .into_iter()
                .next()
                .ok_or(Error::RowNotFound)?;
            Ok(Box::new(output))
        }))
    }

    /// Queues an insert and its typed `RETURNING` result.
    pub fn insert<I>(&mut self, insert: I) -> Result<BatchHandle<I::Output>, Error>
    where
        I: InsertSpec + 'static,
    {
        let statement = insert.compile_batch_insert()?;
        let metadata = validated_row_metadata(&statement)?;

        Ok(self.push(statement, move |statement, result| {
            let rows = rows_from_result(result, statement, metadata)?;
            let output = insert.decode_batch_insert(rows)?;
            Ok(Box::new(output))
        }))
    }

    /// Queues a bulk update and its typed affected-row result.
    pub fn update_many<U>(&mut self, update: U) -> Result<BatchHandle<U::Output>, Error>
    where
        U: UpdateSpec + 'static,
    {
        let statement = update.compile_batch_update()?;

        Ok(self.push(statement, move |statement, result| {
            ensure_batch_result_succeeded(&result)?;
            let changes = changes_from_result(&result, statement)?;
            let output = update.decode_batch_update(changes)?;
            Ok(Box::new(output))
        }))
    }

    /// Queues a bulk delete and its typed affected-row result.
    pub fn delete_many<D>(&mut self, delete: D) -> Result<BatchHandle<D::Output>, Error>
    where
        D: DeleteSpec + 'static,
    {
        let statement = delete.compile_batch_delete()?;

        Ok(self.push(statement, move |statement, result| {
            ensure_batch_result_succeeded(&result)?;
            let changes = changes_from_result(&result, statement)?;
            let output = delete.decode_batch_delete(changes)?;
            Ok(Box::new(output))
        }))
    }

    pub(crate) async fn execute_outputs<O>(self, output: O) -> Result<O::Output, Error>
    where
        O: BatchOutput,
    {
        let handles = output.into_handles()?;
        let mut results = self.execute().await?;
        O::extract(handles, &mut results)
    }

    /// Executes all queued operations through exactly one D1 `batch()` call.
    ///
    /// An empty batch succeeds locally without making a platform call.
    pub async fn execute(self) -> Result<BatchResults, Error> {
        let Self {
            target,
            identity,
            entries,
        } = self;

        if entries.is_empty() {
            return Ok(BatchResults {
                identity,
                slots: Vec::new(),
            });
        }

        // Prepare and bind every statement before invoking D1. Any local
        // binding failure therefore prevents the entire batch from being sent.
        let statements = {
            let executor = target.executor();

            entries
                .iter()
                .map(|entry| prepare_statement(executor, &entry.statement))
                .collect::<Result<Vec<_>, _>>()?
        };

        let results = target.execute(statements).await?;
        validate_result_count(entries.len(), results.len())?;

        let mut slots = Vec::with_capacity(entries.len());

        for (entry, result) in entries.into_iter().zip(results) {
            let value = (entry.decoder)(&entry.statement, result)?;
            slots.push(BatchResultSlot {
                generation: entry.generation,
                value: Some(value),
            });
        }

        Ok(BatchResults { identity, slots })
    }

    fn push<T, F>(&mut self, statement: CompiledStatement, decoder: F) -> BatchHandle<T>
    where
        T: Send + 'static,
        F: FnOnce(&CompiledStatement, D1Result) -> Result<BoxBatchValue, Error> + Send + 'static,
    {
        let slot = self.entries.len();
        let generation = 0;

        self.entries.push(BatchEntry {
            statement,
            generation,
            decoder: Box::new(decoder),
        });

        BatchHandle {
            identity: Arc::clone(&self.identity),
            slot,
            generation,
            _output: PhantomData,
        }
    }
}

/// A typed reference to one operation's output in an [`AtomicBatch`].
///
/// Handles are tied to the batch that created them. Using a handle with another
/// batch's results, or reusing a cloned handle after its value was taken,
/// returns a controlled [`Error::BatchShape`] error.
pub struct BatchHandle<T> {
    identity: Arc<BatchIdentity>,
    slot: usize,
    generation: u64,
    _output: PhantomData<fn() -> T>,
}

impl<T> Clone for BatchHandle<T> {
    fn clone(&self) -> Self {
        Self {
            identity: Arc::clone(&self.identity),
            slot: self.slot,
            generation: self.generation,
            _output: PhantomData,
        }
    }
}

impl<T> fmt::Debug for BatchHandle<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BatchHandle")
            .field("slot", &self.slot)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

struct BatchResultSlot {
    generation: u64,
    value: Option<BoxBatchValue>,
}

/// Fully decoded outputs returned by an executed [`AtomicBatch`].
///
/// Every D1 result is validated and decoded before this value is returned.
pub struct BatchResults {
    identity: Arc<BatchIdentity>,
    slots: Vec<BatchResultSlot>,
}

impl fmt::Debug for BatchResults {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let remaining = self
            .slots
            .iter()
            .filter(|slot| slot.value.is_some())
            .count();

        formatter
            .debug_struct("BatchResults")
            .field("result_count", &self.slots.len())
            .field("remaining", &remaining)
            .finish_non_exhaustive()
    }
}

impl BatchResults {
    /// Returns the number of operation slots in these results.
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Returns `true` when these results contain no operation slots.
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Takes the typed output associated with a batch handle.
    pub fn take<T>(&mut self, handle: BatchHandle<T>) -> Result<T, Error>
    where
        T: Send + 'static,
    {
        if !Arc::ptr_eq(&self.identity, &handle.identity) {
            return Err(Error::BatchShape(
                "batch handle belongs to a different atomic batch".to_owned(),
            ));
        }

        let slot = self.slots.get_mut(handle.slot).ok_or_else(|| {
            Error::BatchShape(format!(
                "batch handle refers to missing result slot {}",
                handle.slot,
            ))
        })?;

        if slot.generation != handle.generation {
            return Err(Error::BatchShape(format!(
                "batch result slot {} has already been taken or the handle is stale",
                handle.slot,
            )));
        }

        let value = slot.value.take().ok_or_else(|| {
            Error::BatchShape(format!(
                "batch result slot {} has already been taken",
                handle.slot,
            ))
        })?;

        slot.generation = slot.generation.saturating_add(1);

        value.downcast::<T>().map(|value| *value).map_err(|_| {
            Error::BatchShape(format!(
                "batch result slot {} does not contain the handle's output type",
                handle.slot,
            ))
        })
    }
}

impl<T> private::Sealed for BatchHandle<T> where T: Send + 'static {}

impl<T> BatchOutput for BatchHandle<T>
where
    T: Send + 'static,
{
    type Output = T;
    type Handles = Self;

    fn into_handles(self) -> Result<Self::Handles, Error> {
        Ok(self)
    }

    fn extract(handle: Self::Handles, results: &mut BatchResults) -> Result<Self::Output, Error> {
        results.take(handle)
    }
}

impl<O> private::Sealed for Result<O, Error> where O: BatchOutput {}

impl<O> BatchOutput for Result<O, Error>
where
    O: BatchOutput,
{
    type Output = O::Output;
    type Handles = O::Handles;

    fn into_handles(self) -> Result<Self::Handles, Error> {
        match self {
            Ok(output) => output.into_handles(),
            Err(error) => Err(error),
        }
    }

    fn extract(handles: Self::Handles, results: &mut BatchResults) -> Result<Self::Output, Error> {
        O::extract(handles, results)
    }
}

impl private::Sealed for () {}

impl BatchOutput for () {
    type Output = ();
    type Handles = ();

    fn into_handles(self) -> Result<Self::Handles, Error> {
        Ok(())
    }

    fn extract((): Self::Handles, _results: &mut BatchResults) -> Result<Self::Output, Error> {
        Ok(())
    }
}

macro_rules! impl_batch_output_tuple {
    ($(($output:ident, $value:ident)),+ $(,)?) => {
        impl<$($output),+> private::Sealed for ($($output,)+)
        where
            $($output: BatchOutput,)+
        {
        }

        impl<$($output),+> BatchOutput for ($($output,)+)
        where
            $($output: BatchOutput,)+
        {
            type Output = ($(<$output as BatchOutput>::Output,)+);
            type Handles = ($(<$output as BatchOutput>::Handles,)+);

            fn into_handles(self) -> Result<Self::Handles, Error> {
                let ($($value,)+) = self;

                $(
                    let $value = $value.into_handles()?;
                )+

                Ok(($($value,)+))
            }

            fn extract(
                handles: Self::Handles,
                results: &mut BatchResults,
            ) -> Result<Self::Output, Error> {
                let ($($value,)+) = handles;

                $(
                    let $value = <$output as BatchOutput>::extract($value, results)?;
                )+

                Ok(($($value,)+))
            }
        }
    };
}

macro_rules! impl_batch_output_tuples {
    (@next [$($implemented:tt)*] []) => {};
    (
        @next
        [$($implemented:tt)*]
        [($output:ident, $value:ident) $(, ($remaining_output:ident, $remaining_value:ident))*]
    ) => {
        impl_batch_output_tuple!($($implemented)* ($output, $value));
        impl_batch_output_tuples!(
            @next
            [$($implemented)* ($output, $value),]
            [$(($remaining_output, $remaining_value)),*]
        );
    };
    ($(($output:ident, $value:ident)),+ $(,)?) => {
        impl_batch_output_tuples!(@next [] [$(($output, $value)),+]);
    };
}

impl_batch_output_tuples!(
    (A, a),
    (B, b),
    (C, c),
    (D, d),
    (E, e),
    (F, f),
    (G, g),
    (H, h),
    (I, i),
    (J, j),
    (K, k),
    (L, l),
    (M, m),
    (N, n),
    (O, o),
    (P, p),
);

fn validated_row_metadata(statement: &CompiledStatement) -> Result<Arc<D1RowMetadata>, Error> {
    D1RowMetadata::new(statement.result_columns()).map(Arc::new)
}

#[derive(Deserialize)]
struct RawBatchRow(#[serde(with = "worker::d1::serde_wasm_bindgen::preserve")] JsValue);

fn ensure_batch_result_succeeded(result: &D1Result) -> Result<(), Error> {
    let error = result.error();

    if result.success() && error.is_none() {
        return Ok(());
    }

    let message = error.unwrap_or_else(|| "no error details were provided".to_owned());

    Err(Error::BatchShape(format!(
        "D1 batch entry returned an unsuccessful result: {message}",
    )))
}

fn rows_from_result(
    result: D1Result,
    statement: &CompiledStatement,
    metadata: Arc<D1RowMetadata>,
) -> Result<Vec<D1Row>, Error> {
    ensure_batch_result_succeeded(&result)?;

    let raw_rows = result.results::<RawBatchRow>()?;
    let mut rows = Vec::with_capacity(raw_rows.len());

    for raw_row in raw_rows {
        rows.push(D1Row::from_named_raw(
            raw_row.0,
            Arc::clone(&metadata),
            statement.result_columns(),
        )?);
    }

    Ok(rows)
}

fn validate_result_count(expected: usize, actual: usize) -> Result<(), Error> {
    if expected == actual {
        Ok(())
    } else {
        Err(Error::BatchShape(format!(
            "D1 returned {actual} batch results for {expected} submitted statements",
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one_result<T>(value: T) -> (BatchResults, BatchHandle<T>)
    where
        T: Send + 'static,
    {
        let identity = Arc::new(BatchIdentity);
        let handle = BatchHandle {
            identity: Arc::clone(&identity),
            slot: 0,
            generation: 0,
            _output: PhantomData,
        };
        let results = BatchResults {
            identity,
            slots: vec![BatchResultSlot {
                generation: 0,
                value: Some(Box::new(value)),
            }],
        };

        (results, handle)
    }

    fn result_handle<T>(identity: &Arc<BatchIdentity>, slot: usize) -> BatchHandle<T> {
        BatchHandle {
            identity: Arc::clone(identity),
            slot,
            generation: 0,
            _output: PhantomData,
        }
    }

    fn batch_results(identity: Arc<BatchIdentity>, values: Vec<BoxBatchValue>) -> BatchResults {
        BatchResults {
            identity,
            slots: values
                .into_iter()
                .map(|value| BatchResultSlot {
                    generation: 0,
                    value: Some(value),
                })
                .collect(),
        }
    }

    fn resolve_outputs<O>(output: O, results: &mut BatchResults) -> Result<O::Output, Error>
    where
        O: BatchOutput,
    {
        let handles = output.into_handles()?;
        O::extract(handles, results)
    }

    #[test]
    fn batch_outputs_resolve_heterogeneous_tuple_in_order() {
        let identity = Arc::new(BatchIdentity);
        let number = result_handle::<u64>(&identity, 0);
        let text = result_handle::<String>(&identity, 1);
        let flags = result_handle::<Vec<bool>>(&identity, 2);
        let mut results = batch_results(
            identity,
            vec![
                Box::new(7_u64),
                Box::new("decoded".to_owned()),
                Box::new(vec![true, false]),
            ],
        );

        let output = resolve_outputs(
            (
                Ok::<_, Error>(number),
                Ok::<_, Error>(text),
                Ok::<_, Error>(flags),
            ),
            &mut results,
        )
        .expect("heterogeneous outputs should resolve");

        assert_eq!(output, (7_u64, "decoded".to_owned(), vec![true, false]));
    }

    #[test]
    fn batch_outputs_follow_callback_order_instead_of_slot_order() {
        let identity = Arc::new(BatchIdentity);
        let number = result_handle::<u64>(&identity, 0);
        let text = result_handle::<String>(&identity, 1);
        let flag = result_handle::<bool>(&identity, 2);
        let mut results = batch_results(
            identity,
            vec![
                Box::new(41_u64),
                Box::new("second slot".to_owned()),
                Box::new(true),
            ],
        );

        let output = resolve_outputs(
            (
                Ok::<_, Error>(flag),
                Ok::<_, Error>(number),
                Ok::<_, Error>(text),
            ),
            &mut results,
        )
        .expect("reordered handles should resolve");

        assert_eq!(output, (true, 41_u64, "second slot".to_owned()));
    }

    #[test]
    fn batch_output_queue_error_prevents_result_extraction() {
        let identity = Arc::new(BatchIdentity);
        let handle = result_handle::<u64>(&identity, 0);
        let retained_handle = handle.clone();
        let mut results = batch_results(identity, vec![Box::new(7_u64)]);
        let queued = (
            Ok::<_, Error>(handle),
            Err::<BatchHandle<String>, Error>(Error::Binding(
                "second queue operation failed".to_owned(),
            )),
            Err::<BatchHandle<bool>, Error>(Error::Binding(
                "third queue operation failed".to_owned(),
            )),
        );

        let error = queued
            .into_handles()
            .expect_err("the first queue error should be returned");

        assert!(matches!(
            error,
            Error::Binding(ref message) if message == "second queue operation failed"
        ));
        assert_eq!(
            results
                .take(retained_handle)
                .expect("queue conversion must not extract result slots"),
            7_u64
        );
    }

    #[test]
    fn batch_outputs_support_unit_and_single_operation_shapes() {
        let identity = Arc::new(BatchIdentity);
        let mut empty_results = batch_results(identity, Vec::new());

        assert_eq!(
            resolve_outputs((), &mut empty_results).expect("unit output should resolve"),
            ()
        );

        let (mut single_results, single_handle) = one_result("single".to_owned());
        assert_eq!(
            resolve_outputs(Ok::<_, Error>(single_handle), &mut single_results)
                .expect("a single queue result should resolve"),
            "single"
        );

        let (mut tuple_results, tuple_handle) = one_result(23_u64);
        assert_eq!(
            resolve_outputs((Ok::<_, Error>(tuple_handle),), &mut tuple_results)
                .expect("a one-element tuple should resolve"),
            (23_u64,)
        );
    }

    #[test]
    fn batch_outputs_support_fallible_callback_tuples() {
        let identity = Arc::new(BatchIdentity);
        let number = result_handle::<u64>(&identity, 0);
        let text = result_handle::<String>(&identity, 1);
        let mut results = batch_results(
            identity,
            vec![Box::new(9_u64), Box::new("callback".to_owned())],
        );

        let callback_output = (|| {
            let number = Ok::<_, Error>(number)?;
            let text = Ok::<_, Error>(text)?;

            Ok((number, text))
        })();

        let output = resolve_outputs(callback_output, &mut results)
            .expect("a fallible callback tuple should resolve");

        assert_eq!(output, (9_u64, "callback".to_owned()));
    }

    #[test]
    fn result_count_must_match_submitted_statement_count() {
        validate_result_count(4, 4).expect("matching result counts should be accepted");

        let error =
            validate_result_count(4, 3).expect_err("missing batch results should be rejected");

        assert!(matches!(error, Error::BatchShape(_)));
        assert!(
            error
                .to_string()
                .contains("3 batch results for 4 submitted statements")
        );
    }

    #[test]
    fn typed_results_reject_handles_from_another_batch() {
        let (mut results, _) = one_result(7_u64);
        let (_, foreign_handle) = one_result(9_u64);

        let error = results
            .take(foreign_handle)
            .expect_err("foreign handle should be rejected");

        assert!(matches!(error, Error::BatchShape(_)));
        assert!(error.to_string().contains("different atomic batch"));
    }

    #[test]
    fn typed_results_reject_reused_handles() {
        let (mut results, handle) = one_result("decoded".to_owned());
        let duplicate = handle.clone();

        assert_eq!(
            results.take(handle).expect("first take should succeed"),
            "decoded"
        );

        let error = results
            .take(duplicate)
            .expect_err("reused handle should be rejected");

        assert!(matches!(error, Error::BatchShape(_)));
        assert!(error.to_string().contains("already been taken"));
    }
}
