mod info;
mod query;

use core::fmt;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
};

pub use info::TypeTagInfo;
pub use query::TypeTagQuery;

#[derive(Debug, Clone)]
pub struct TypeTag {
    hash: u64,
    info: Arc<TypeTagInfo<'static>>,
}

impl TypeTag {
    pub fn info(&self) -> &TypeTagInfo<'static> {
        &self.info
    }
}

impl From<TypeTagInfo<'static>> for TypeTag {
    fn from(info: TypeTagInfo<'static>) -> Self {
        let mut hasher = DefaultHasher::new();
        info.hash(&mut hasher);

        Self {
            info: Arc::new(info),
            hash: hasher.finish(),
        }
    }
}

impl PartialEq for TypeTag {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for TypeTag {}

impl fmt::Display for TypeTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.info, f)
    }
}

#[derive(Debug, Clone, Default)]
pub struct TypeTagFilterMap {
    queries: Vec<TypeTagQuery<'static>>,
}

impl TypeTagFilterMap {
    pub fn new(queries: &[&str]) -> Self {
        let queries = queries
            .iter()
            .map(|&query| TypeTagQuery::parse(query).unwrap().make_owned())
            .collect();

        Self { queries }
    }

    pub fn from_static(queries: &[&'static str]) -> Self {
        let queries = queries
            .iter()
            .map(|&query| TypeTagQuery::parse(query).unwrap())
            .collect();

        Self { queries }
    }

    pub fn add_query(&mut self, query: &str) -> bool {
        let query = if let Some(query) = TypeTagQuery::parse(query) {
            query
        } else {
            return false;
        };

        self.queries.push(query.make_owned());
        true
    }

    pub fn add_query_static(&mut self, query: &'static str) -> bool {
        let query = if let Some(query) = TypeTagQuery::parse(query) {
            query
        } else {
            return false;
        };

        self.queries.push(query);
        true
    }

    pub fn match_any(&self, tt: &TypeTagInfo) -> bool {
        for query in &self.queries {
            if query.test(tt) {
                return true;
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use crate::type_tag::TypeTagInfo;

    use super::TypeTagFilterMap;

    #[test]
    fn test_tt_filter_map() {
        let fm = TypeTagFilterMap::from_static(&[
            "api::Request<~>",
            "api::Response<~>",
            "nn::image::Resizer",
            "nn::classifier::*",
            "log::**",
        ]);

        assert!(fm.match_any(&TypeTagInfo::parse("api::Request<Login>").unwrap()));
        assert!(fm
            .match_any(&TypeTagInfo::parse("api::Response<api::data::Paginated<Users>>").unwrap()));
        assert!(fm.match_any(&TypeTagInfo::parse("nn::classifier::ResNet").unwrap()));
        assert!(!fm.match_any(&TypeTagInfo::parse("nn::detector::Yolo").unwrap()));
        assert!(fm.match_any(&TypeTagInfo::parse("log::login::FailedLoginError").unwrap()));
    }
}
