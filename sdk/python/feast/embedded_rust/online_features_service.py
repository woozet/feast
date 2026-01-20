import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import pyarrow as pa
from google.protobuf.timestamp_pb2 import Timestamp

from feast.errors import (
    FeatureNameCollisionError,
    RequestDataNotFoundInEntityRowsException,
)
from feast.feature_service import FeatureService
from feast.online_response import OnlineResponse
from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesResponse
from feast.protos.feast.types import Value_pb2
from feast.types import from_value_type
from feast.value_type import ValueType

from .lib.embedded import ffi, lib
from .lib.rust import CStringArray
from .type_map import FEAST_TYPE_TO_ARROW_TYPE, arrow_array_to_array_of_proto

logger = logging.getLogger(__name__)


class EmbeddedRustOnlineFeatureServer:
    def __init__(self, repo_path: str):
        err = ffi.new("char **")
        service = lib.feast_rust_new_service(repo_path.encode(), err)
        if service == ffi.NULL:
            raise RuntimeError(_consume_error(err))
        self._service = service

    def __del__(self):
        try:
            service = getattr(self, "_service", None)
            ffi_handle = globals().get("ffi")
            lib_handle = globals().get("lib")
            if (
                service is None
                or ffi_handle is None
                or lib_handle is None
                or getattr(ffi_handle, "NULL", None) is None
            ):
                return
            if service != ffi_handle.NULL:
                lib_handle.feast_rust_free_service(service)
                self._service = ffi_handle.NULL
        except Exception:
            # Avoid interpreter-shutdown errors from cffi internals.
            return

    def get_online_features(
        self,
        features_refs: List[str],
        feature_service: Optional[FeatureService],
        entities: Dict[str, Union[List[Any], Value_pb2.RepeatedValue]],
        request_data: Dict[str, Union[List[Any], Value_pb2.RepeatedValue]],
        full_feature_names: bool = False,
    ):
        feature_service_name = feature_service.name if feature_service else ""

        feature_refs = CStringArray(features_refs)

        entities_batch, entities_schema = map_to_record_batch(entities)
        request_batch, request_schema = map_to_record_batch(request_data)

        (
            entities_c_schema,
            entities_ptr_schema,
            entities_c_array,
            entities_ptr_array,
        ) = _export_record_batch(entities_batch, entities_schema)
        (
            request_c_schema,
            request_ptr_schema,
            request_c_array,
            request_ptr_array,
        ) = _export_record_batch(request_batch, request_schema)

        output_schema, output_ptr_schema, output_array, output_ptr_array = (
            allocate_schema_and_array()
        )
        output = ffi.new(
            "DataTable *",
            {"schema": output_schema, "array": output_array},
        )
        entities_table = ffi.new(
            "DataTable *", {"schema": entities_c_schema, "array": entities_c_array}
        )
        request_table = ffi.new(
            "DataTable *", {"schema": request_c_schema, "array": request_c_array}
        )

        err = ffi.new("char **")
        ok = lib.feast_rust_get_online_features(
            self._service,
            feature_refs.ptr,
            feature_refs.length,
            feature_service_name.encode(),
            entities_table[0],
            request_table[0],
            full_feature_names,
            output,
            err,
        )
        if not ok:
            raise RuntimeError(_consume_error(err))

        record_batch = pa.RecordBatch._import_from_c(output_ptr_array, output_ptr_schema)
        resp = record_batch_to_online_response(record_batch)
        del record_batch
        return OnlineResponse(resp)


def _consume_error(err_ptr) -> str:
    if err_ptr == ffi.NULL or err_ptr[0] == ffi.NULL:
        return "unknown error"
    message = ffi.string(err_ptr[0]).decode()
    lib.feast_rust_free_string(err_ptr[0])
    err_ptr[0] = ffi.NULL
    return message


def allocate_schema_and_array():
    c_schema = ffi.new("struct ArrowSchema*")
    ptr_schema = int(ffi.cast("uintptr_t", c_schema))
    c_array = ffi.new("struct ArrowArray*")
    ptr_array = int(ffi.cast("uintptr_t", c_array))
    return c_schema, ptr_schema, c_array, ptr_array


def _export_record_batch(batch: pa.RecordBatch, schema: pa.Schema):
    c_schema, ptr_schema, c_array, ptr_array = allocate_schema_and_array()
    schema._export_to_c(ptr_schema)
    batch._export_to_c(ptr_array)
    return c_schema, ptr_schema, c_array, ptr_array


def map_to_record_batch(
    map: Dict[str, Union[List[Any], Value_pb2.RepeatedValue]],
    type_hint: Optional[Dict[str, ValueType]] = None,
) -> Tuple[pa.RecordBatch, pa.Schema]:
    fields = []
    columns = []
    type_hint = type_hint or {}

    for name, values in map.items():
        arr = _to_arrow(values, type_hint.get(name))
        fields.append((name, arr.type))
        columns.append(arr)

    schema = pa.schema(fields)
    batch = pa.RecordBatch.from_arrays(columns, schema=schema)
    return batch, schema


def record_batch_to_online_response(record_batch):
    resp = GetOnlineFeaturesResponse()

    for idx, field in enumerate(record_batch.schema):
        if field.name.endswith("__timestamp") or field.name.endswith("__status"):
            continue

        feature_vector = GetOnlineFeaturesResponse.FeatureVector(
            statuses=record_batch.columns[idx + 1].to_pylist(),
            event_timestamps=[
                Timestamp(seconds=seconds)
                for seconds in record_batch.columns[idx + 2].to_pylist()
            ],
        )

        if field.type == pa.null():
            feature_vector.values.extend(
                [Value_pb2.Value()] * len(record_batch.columns[idx])
            )
        else:
            feature_vector.values.extend(
                arrow_array_to_array_of_proto(field.type, record_batch.columns[idx])
            )

        resp.results.append(feature_vector)
        resp.metadata.feature_names.val.append(field.name)

    return resp


def _to_arrow(value, type_hint: Optional[ValueType]) -> pa.Array:
    if isinstance(value, Value_pb2.RepeatedValue):
        _proto_to_arrow(value)

    if type_hint:
        feast_type = from_value_type(type_hint)
        if feast_type in FEAST_TYPE_TO_ARROW_TYPE:
            return pa.array(value, FEAST_TYPE_TO_ARROW_TYPE[feast_type])

    return pa.array(value)


def _proto_to_arrow(value: Value_pb2.RepeatedValue) -> pa.Array:
    """
    ToDo: support entity rows already packed in protos
    """
    raise NotImplementedError
