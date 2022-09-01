/*
 * Copyright (C) 2021 Vaticle
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use grpc::{Error as GrpcError, GrpcMessageError, GrpcStatus};
use std::error::Error as StdError;
use std::fmt::{Debug, Display, Formatter};

pub trait ErrorMessage {
    const CODE_PREFIX: &'static str;
    const MESSAGE_PREFIX: &'static str;
    const CODE_DIGITS: usize;

    fn code_number(&self) -> usize;
    fn message_body(&self) -> &'static str;

    fn template(&self) -> String {
        format!(
            "[{}{}{}] {}: {}",
            Self::CODE_PREFIX,
            "0".repeat(Self::CODE_DIGITS - num_digits(self.code_number())),
            self.code_number(),
            Self::MESSAGE_PREFIX,
            self.message_body()
        )
    }

    fn format(&self, args: &[&str]) -> String {
        let expected_arg_count = self.message_body().matches("{}").count();
        assert_eq!(
            expected_arg_count, args.len(),
            "Message template `{}` takes `{}` args but `{}` were provided",
            self.message_body(), expected_arg_count, args.len()
        );
        let mut buffer = String::new();
        for (i, fragment) in self.template().split("{}").enumerate() {
            if i > 0 {
                buffer += args.get(i - 1).unwrap_or(&"{}");
            }
            buffer += fragment;
        }
        buffer
    }

    fn to_err(&self, args: &[&str]) -> Error {
        Error::new(self.format(args))
    }
}

const fn num_digits(x: usize) -> usize {
    if x < 10 {
        1
    } else {
        1 + num_digits(x / 10)
    }
}

macro_rules! last {
    ($x:tt) => ($x);
    ($x:tt $($xs:tt)*) => (last!($($xs)*));
}

macro_rules! error_message {
    ($name:ident, $code_pfx:literal, $message_pfx:literal, $(($error_name:ident, $code:literal, $body:literal)),*) => {
        pub struct $name {
            code_number: usize,
            message_body: &'static str,
        }
        impl ErrorMessage for $name {
            const CODE_PREFIX: &'static str = $code_pfx;
            const MESSAGE_PREFIX: &'static str = $message_pfx;
            const CODE_DIGITS: usize = num_digits(last!($($code),*));

            fn code_number(&self) -> usize {
                self.code_number
            }
            fn message_body(&self) -> &'static str {
                self.message_body
            }
        }
        $(
            pub const $error_name: $name = $name { code_number: $code, message_body: $body };
        )*
    }
}

error_message!(
    ClientMessage, "CLI", "Client Error",
    (SESSION_IS_CLOSED, 2, "The session is closed and no further operation is allowed."),
    (TRANSACTION_IS_CLOSED, 3, "The transaction is closed and no further operation is allowed."),
    (TRANSACTION_IS_CLOSED_WITH_ERRORS, 4, "The transaction is closed because of the error(s):\n{}"),
    (UNABLE_TO_CONNECT, 5, "Unable to connect to TypeDB server."),
    (DB_DOES_NOT_EXIST, 8, "The database '{}' does not exist."),
    (MISSING_RESPONSE_FIELD, 9, "Missing field in message received from server: '{}'."),
    (UNKNOWN_REQUEST_ID, 10, "Received a response with unknown request id '{}':\n{}"),
    (CLUSTER_REPLICA_NOT_PRIMARY, 13, "The replica is not the primary replica."),
    (CLUSTER_TOKEN_CREDENTIAL_INVALID, 16, "Invalid token credential."),
    (SESSION_CLOSE_FAILED, 17, "Failed to close session. It may still be open on the server, or it may already have been closed previously.")
);

error_message!(
    ConceptMessage, "CON", "Concept Error",
    (INVALID_CONCEPT_CASTING, 1, "invalid concept conversion from '{}' to '{}'")
);

//////////////////////////////////////

#[derive(Debug)]
pub enum Error {
    GrpcError(String, GrpcError),
    Other(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Error::GrpcError(msg, _) => msg,
            Error::Other(msg) => msg,
        };
        write!(f, "{}", message)
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Error::GrpcError(_, source) => Some(source),
            Error::Other(_) => None,
        }
    }
}

impl Error {
    pub(crate) fn new(msg: String) -> Self {
        Error::Other(msg)
    }

    pub(crate) fn from_grpc(source: GrpcError) -> Self {
        match source {
            GrpcError::Http(_) => Error::GrpcError(UNABLE_TO_CONNECT.format(&[]), source),
            GrpcError::GrpcMessage(ref err) => {
                // TODO: this is awkward because we use gRPC errors to represent some user errors too
                if Error::is_replica_not_primary(err) {
                    Error::new(CLUSTER_REPLICA_NOT_PRIMARY.format(&[]))
                } else if Error::is_token_credential_invalid(err) {
                    Error::new(CLUSTER_TOKEN_CREDENTIAL_INVALID.format(&[]))
                } else {
                    Error::GrpcError(
                        source.to_string().replacen("grpc message error: ", "", 1),
                        source,
                    )
                }
            }
            _ => Error::GrpcError(source.to_string(), source),
        }
    }

    fn is_replica_not_primary(err: &GrpcMessageError) -> bool {
        err.grpc_status == GrpcStatus::Internal as i32 && err.grpc_message.contains("[RPL01]")
    }

    fn is_token_credential_invalid(err: &GrpcMessageError) -> bool {
        err.grpc_status == GrpcStatus::Unauthenticated as i32
            && err.grpc_message.contains("[CLS08]")
    }
}
