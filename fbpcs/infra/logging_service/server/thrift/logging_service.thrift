/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/*
 * Run thrift compiler (Apache thrift or fbthrift) in the parent folder:
 *   thrift -r --gen py -out . thrift/logging_service.thrift
 */

namespace hack PrivateComputation
namespace java meta.private_computation
namespace py meta.private_computation

exception InvalidRequestError {
  1: string external_error_message;
}

exception InternalServerError {
  1: string external_error_message;
}

exception NotFoundError {
  1: string error_message;
}

struct PutMetadataRequest {
  1: string partner_id;
  2: string entity_key;
  3: string entity_value;
}

struct PutMetadataResponse {
  1: bool success;
}

struct GetMetadataRequest {
  1: string partner_id;
  2: string entity_key;
}

struct GetMetadataResponse {
  1: string entity_value;
}

struct ListMetadataRequest {
  1: string partner_id;
  2: string entity_key_start;
  3: string entity_key_end;
  4: i32 result_limit;
}

struct ListMetadataResponse {
  1: map<string, string> key_values;
}

service LoggingService {
  PutMetadataResponse putMetadata(1: PutMetadataRequest request) throws (
    1: InvalidRequestError invalid_request_error,
    2: InternalServerError internal_server_error,
  );
  GetMetadataResponse getMetadata(1: GetMetadataRequest request) throws (
    1: InvalidRequestError invalid_request_error,
    2: InternalServerError internal_server_error,
    3: NotFoundError not_found_error,
  );
  ListMetadataResponse listMetadata(1: ListMetadataRequest request) throws (
    1: InvalidRequestError invalid_request_error,
    2: InternalServerError internal_server_error,
  );
}
