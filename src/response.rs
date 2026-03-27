#![allow(dead_code)]
use axum::{http::StatusCode, response::IntoResponse, Json};
use serde::Serialize;

use crate::AppError;

pub struct ResponseBuilder<T: Serialize> {
    response: Response<T>,
}

#[derive(Serialize)]
struct Response<T: Serialize> {
    data: Option<T>,
    error: Option<AppError>,
    message: String,
    links: Vec<ResponseLink>,
}

impl<T: Serialize> ResponseBuilder<T> {
    pub fn new(data: T) -> Self {
        Self {
            response: Response {
                data: Some(data),
                error: None,
                message: String::new(),
                links: vec![],
            },
        }
    }

    pub fn message<S: Into<String>>(self, message: S) -> Self {
        let response = Response {
            message: message.into(),
            ..self.response
        };
        Self { response }
    }

    pub fn link(self, link: ResponseLink) -> Self {
        let mut links = self.response.links;
        links.push(link);
        let response = Response {
            links,
            ..self.response
        };
        Self { response }
    }

    pub fn error(error: AppError) -> Self {
        Self {
            response: Response {
                data: None,
                error: Some(error),
                message: String::new(),
                links: vec![],
            },
        }
    }

    pub fn json(self, code: StatusCode) -> impl IntoResponse {
        (code, Json(self.response)).into_response()
    }
}

#[derive(Serialize)]
pub struct ResponseLink {
    pub rel: String,
    pub href: String,
    pub method: String,
}

impl ResponseLink {
    pub fn new<S: Into<String>>(rel: S, href: S, method: S) -> Self {
        Self {
            rel: rel.into(),
            href: href.into(),
            method: method.into(),
        }
    }
}
