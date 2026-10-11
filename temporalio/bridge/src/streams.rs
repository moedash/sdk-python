//! The stream service Core serves in process, reached from Python.

use std::sync::Arc;

use prost::Message;
use pyo3::{create_exception, exceptions::PyException, exceptions::PyValueError, prelude::*};
use temporalio_sdk_core::streams::{connect_stream_service, proto, StreamService};

use crate::{client::ClientRef, runtime};

// Carries a serialized `coresdk.streams.StreamFailure`, so Python maps every failure the same way
// whatever store raised it.
create_exception!(temporal_sdk_bridge, StreamFailureError, PyException);

/// One process-wide stream service over the configured store. Python passes it in a Worker's
/// config, which takes a copy.
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct StreamStoreRef {
    pub(crate) service: Arc<StreamService>,
    runtime: runtime::Runtime,
}

/// Connects to the store a serialized `StreamStoreConfig` names, asking `client`'s server about
/// streams' owners.
#[pyfunction]
pub fn connect_stream_store<'p>(
    py: Python<'p>,
    client: &ClientRef,
    config: Vec<u8>,
) -> PyResult<Bound<'p, PyAny>> {
    client
        .runtime
        .assert_same_process("connect a stream store")?;
    let config = proto::StreamStoreConfig::decode(&*config)
        .map_err(|err| PyValueError::new_err(format!("Invalid stream store config: {err}")))?;
    let connection = client.connection.clone();
    let runtime = client.runtime.clone();
    client.runtime.future_into_py(py, async move {
        let service = connect_stream_service(config, connection)
            .await
            .map_err(|err| failure_error(proto::StreamFailure::from(err)))?;
        Ok(StreamStoreRef { service, runtime })
    })
}

#[pymethods]
impl StreamStoreRef {
    /// Makes one stream service call with a serialized request, answering with the serialized
    /// response.
    fn call<'p>(
        &self,
        py: Python<'p>,
        rpc: String,
        request: Vec<u8>,
    ) -> PyResult<Bound<'p, PyAny>> {
        self.runtime.assert_same_process("call a stream store")?;
        let service = self.service.clone();
        self.runtime.future_into_py(py, async move {
            service.call(&rpc, &request).await.map_err(failure_error)
        })
    }
}

fn failure_error(failure: proto::StreamFailure) -> PyErr {
    StreamFailureError::new_err(failure.encode_to_vec())
}
