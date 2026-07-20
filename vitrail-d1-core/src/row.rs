use std::collections::HashMap;
use std::sync::Arc;

use vitrail_sqlite_dialect::ResultColumn;
use worker::js_sys::{Array, ArrayBuffer, Reflect, Uint8Array};
use worker::wasm_bindgen::{JsCast, JsValue};

use crate::Error;

#[derive(Debug)]
pub(crate) struct D1RowMetadata {
    aliases: HashMap<String, usize>,
}

impl D1RowMetadata {
    pub(crate) fn new(columns: &[ResultColumn]) -> Result<Self, Error> {
        let mut aliases = HashMap::with_capacity(columns.len());

        for (index, column) in columns.iter().enumerate() {
            if aliases.insert(column.alias().to_owned(), index).is_some() {
                return Err(Error::decode(format!(
                    "compiled D1 result metadata contains duplicate alias `{}`",
                    column.alias(),
                )));
            }
        }

        Ok(Self { aliases })
    }

    fn len(&self) -> usize {
        self.aliases.len()
    }

    fn index(&self, alias: &str) -> Option<usize> {
        self.aliases.get(alias).copied()
    }
}

/// A positional D1 result row mapped to Vitrail's compiled column aliases.
///
/// D1 rows are retained as JavaScript values and decoded through controlled
/// scalar conversions instead of being deserialized directly into user models.
#[derive(Clone, Debug)]
pub struct D1Row {
    metadata: Arc<D1RowMetadata>,
    values: Vec<JsValue>,
}

impl D1Row {
    pub(crate) fn from_raw(raw: JsValue, metadata: Arc<D1RowMetadata>) -> Result<Self, Error> {
        if !Array::is_array(&raw) {
            return Err(Error::decode(format!(
                "expected a positional D1 result row, got {}",
                js_type_name(&raw),
            )));
        }

        let row = Array::from(&raw);
        let actual = row.length() as usize;
        let expected = metadata.len();

        if actual != expected {
            return Err(Error::decode(format!(
                "D1 result row contains {actual} values but the compiled statement describes {expected} columns",
            )));
        }

        Ok(Self {
            metadata,
            values: row.to_vec(),
        })
    }

    pub(crate) fn from_named_raw(
        raw: JsValue,
        metadata: Arc<D1RowMetadata>,
        columns: &[ResultColumn],
    ) -> Result<Self, Error> {
        if raw.is_null() || raw.is_undefined() || Array::is_array(&raw) || !raw.is_object() {
            return Err(Error::decode(format!(
                "expected a named D1 batch result row object, got {}",
                js_type_name(&raw),
            )));
        }

        let keys = Reflect::own_keys(&raw)
            .map_err(|_| Error::decode("could not inspect properties on a D1 batch result row"))?;
        let actual = keys.length() as usize;
        let expected = metadata.len();

        if actual != expected {
            return Err(Error::decode(format!(
                "D1 batch result row contains {actual} fields but the compiled statement describes {expected} columns",
            )));
        }

        if columns.len() != expected {
            return Err(Error::decode(format!(
                "compiled D1 result metadata contains {expected} aliases but describes {} ordered columns",
                columns.len(),
            )));
        }

        let mut values = Vec::with_capacity(expected);

        for column in columns {
            let alias = column.alias();
            let key = JsValue::from_str(alias);
            let present = Reflect::has(&raw, &key).map_err(|_| {
                Error::decode(format!(
                    "could not inspect compiled column alias `{alias}` on a D1 batch result row",
                ))
            })?;

            if !present {
                return Err(Error::decode(format!(
                    "D1 batch result row is missing compiled column alias `{alias}`",
                )));
            }

            let value = Reflect::get(&raw, &key).map_err(|_| {
                Error::decode(format!(
                    "could not read compiled column alias `{alias}` from a D1 batch result row",
                ))
            })?;
            values.push(value);
        }

        Ok(Self { metadata, values })
    }

    pub(crate) fn value(&self, alias: &str) -> Result<&JsValue, Error> {
        self.metadata
            .index(alias)
            .and_then(|index| self.values.get(index))
            .ok_or_else(|| {
                Error::decode(format!(
                    "D1 result row is missing compiled column alias `{alias}`",
                ))
            })
    }

    pub(crate) fn is_null(&self, alias: &str) -> Result<bool, Error> {
        Ok(self.value(alias)?.is_null())
    }

    pub(crate) fn decode_i64(&self, alias: &str) -> Result<i64, Error> {
        let value = self.value(alias)?;
        let text = value
            .as_string()
            .ok_or_else(|| scalar_type_error(alias, "a decimal integer string", value))?;

        text.parse::<i64>().map_err(|error| {
            Error::decode_with_source(
                format!("column `{alias}` contains invalid signed 64-bit integer text `{text}`"),
                error,
            )
        })
    }

    pub(crate) fn decode_bool(&self, alias: &str) -> Result<bool, Error> {
        let value = self.value(alias)?;

        if let Some(value) = value.as_bool() {
            return Ok(value);
        }

        if let Some(value) = value.as_f64() {
            if value.is_finite() {
                return Ok(value != 0.0);
            }

            return Err(Error::decode(format!(
                "column `{alias}` contains a non-finite numeric boolean",
            )));
        }

        Err(scalar_type_error(
            alias,
            "a boolean or finite number",
            value,
        ))
    }

    pub(crate) fn decode_f64(&self, alias: &str) -> Result<f64, Error> {
        let value = self.value(alias)?;
        let number = value
            .as_f64()
            .ok_or_else(|| scalar_type_error(alias, "a finite number", value))?;

        if !number.is_finite() {
            return Err(Error::decode(format!(
                "column `{alias}` contains a non-finite floating-point value",
            )));
        }

        Ok(number)
    }

    pub(crate) fn decode_string(&self, alias: &str) -> Result<String, Error> {
        let value = self.value(alias)?;

        value
            .as_string()
            .ok_or_else(|| scalar_type_error(alias, "a string", value))
    }

    pub(crate) fn decode_bytes(&self, alias: &str) -> Result<Vec<u8>, Error> {
        decode_js_bytes(self.value(alias)?, alias)
    }

    pub(crate) fn decode_json_text(&self, alias: &str) -> Result<serde_json::Value, Error> {
        let text = self.decode_string(alias)?;

        serde_json::from_str(&text).map_err(|error| {
            Error::decode_with_source(
                format!("column `{alias}` contains invalid JSON text"),
                error,
            )
        })
    }
}

fn decode_js_bytes(value: &JsValue, alias: &str) -> Result<Vec<u8>, Error> {
    if let Some(buffer) = value.dyn_ref::<ArrayBuffer>() {
        return Ok(Uint8Array::new(buffer).to_vec());
    }

    if ArrayBuffer::is_view(value) {
        let buffer = Reflect::get(value, &JsValue::from_str("buffer")).map_err(|_| {
            Error::decode(format!(
                "typed-array BLOB in column `{alias}` does not expose its backing buffer",
            ))
        })?;
        let buffer = buffer.dyn_into::<ArrayBuffer>().map_err(|_| {
            Error::decode(format!(
                "typed-array BLOB in column `{alias}` has an invalid backing buffer",
            ))
        })?;
        let byte_offset = js_u32_property(value, "byteOffset", alias)?;
        let byte_length = js_u32_property(value, "byteLength", alias)?;
        let bytes = Uint8Array::new_with_byte_offset_and_length(&buffer, byte_offset, byte_length);

        return Ok(bytes.to_vec());
    }

    if Array::is_array(value) {
        let values = Array::from(value);
        let mut bytes = Vec::with_capacity(values.length() as usize);

        for (index, value) in values.iter().enumerate() {
            let Some(number) = value.as_f64() else {
                return Err(Error::decode(format!(
                    "byte array in column `{alias}` contains a non-numeric value at index {index}",
                )));
            };

            if !number.is_finite()
                || number.fract() != 0.0
                || !(0.0..=u8::MAX as f64).contains(&number)
            {
                return Err(Error::decode(format!(
                    "byte array in column `{alias}` contains invalid byte `{number}` at index {index}",
                )));
            }

            bytes.push(number as u8);
        }

        return Ok(bytes);
    }

    Err(scalar_type_error(
        alias,
        "an ArrayBuffer, typed array, or byte array",
        value,
    ))
}

fn js_u32_property(value: &JsValue, property: &str, alias: &str) -> Result<u32, Error> {
    let property_value = Reflect::get(value, &JsValue::from_str(property)).map_err(|_| {
        Error::decode(format!(
            "typed-array BLOB in column `{alias}` does not expose `{property}`",
        ))
    })?;
    let number = property_value.as_f64().ok_or_else(|| {
        Error::decode(format!(
            "typed-array BLOB in column `{alias}` has a non-numeric `{property}`",
        ))
    })?;

    if !number.is_finite() || number.fract() != 0.0 || !(0.0..=u32::MAX as f64).contains(&number) {
        return Err(Error::decode(format!(
            "typed-array BLOB in column `{alias}` has invalid `{property}` value `{number}`",
        )));
    }

    Ok(number as u32)
}

fn scalar_type_error(alias: &str, expected: &str, value: &JsValue) -> Error {
    Error::decode(format!(
        "column `{alias}` expected {expected}, got {}",
        js_type_name(value),
    ))
}

fn js_type_name(value: &JsValue) -> String {
    if value.is_null() {
        return "null".to_owned();
    }

    if value.is_undefined() {
        return "undefined".to_owned();
    }

    if Array::is_array(value) {
        return "array".to_owned();
    }

    if value.dyn_ref::<ArrayBuffer>().is_some() {
        return "ArrayBuffer".to_owned();
    }

    if ArrayBuffer::is_view(value) {
        return "typed array".to_owned();
    }

    value
        .js_typeof()
        .as_string()
        .unwrap_or_else(|| "unknown JavaScript value".to_owned())
}
