use core::fmt;
use std::{
    borrow::Cow,
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    iter,
};

use lazy_static::lazy_static;
use regex::Regex;

use super::TypeTagInfo;

lazy_static! {
    static ref QUERY_RE: Regex = Regex::new(
        r"^\s*((?:\s*(?:[a-zA-Z_][a-zA-Z0-9_]*|\*\*?)::)*)\s*(?:(\*\*)|(?:([a-zA-Z_][a-zA-Z0-9_]*|\*)(?:<(.*)>)?))\s*(.*)$"
    )
    .unwrap();
}
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum QueryEntry<T> {
    Value(T),
    Any(bool),
}

impl<T: fmt::Display> fmt::Display for QueryEntry<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryEntry::Value(val) => write!(f, "{}", val),
            QueryEntry::Any(period) => write!(f, "{}", if *period { "**" } else { "*" }),
        }
    }
}

impl<T> QueryEntry<T> {
    pub fn is_any(&self) -> bool {
        match self {
            QueryEntry::Value(..) => false,
            QueryEntry::Any(..) => true,
        }
    }

    pub fn map<'a, U, F: FnMut(&'a T) -> U>(&'a self, mut f: F) -> QueryEntry<U> {
        match self {
            QueryEntry::Value(x) => QueryEntry::Value(f(x)),
            QueryEntry::Any(p) => QueryEntry::Any(*p),
        }
    }

    #[inline]
    pub fn make_any(&mut self) {
        *self = QueryEntry::Any(false)
    }

    #[inline]
    pub fn make_any_period(&mut self) {
        *self = QueryEntry::Any(true)
    }
}

impl<'s> QueryEntry<Cow<'s, str>> {
    pub fn as_owned(&self) -> QueryEntry<Cow<'static, str>> {
        match self {
            QueryEntry::Value(Cow::Owned(x)) => QueryEntry::Value(Cow::Owned(x.to_string())),
            QueryEntry::Value(Cow::Borrowed(x)) => QueryEntry::Value(Cow::Owned(x.to_string())),
            QueryEntry::Any(p) => QueryEntry::Any(*p),
        }
    }

    pub fn as_borrowed(&self) -> QueryEntry<Cow<'_, str>> {
        match self {
            QueryEntry::Value(Cow::Owned(x)) => QueryEntry::Value(Cow::Borrowed(x.as_str())),
            QueryEntry::Value(Cow::Borrowed(x)) => QueryEntry::Value(Cow::Borrowed(x)),
            QueryEntry::Any(p) => QueryEntry::Any(*p),
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct TypeTagQuery<'s> {
    pub namespace: QueryEntry<Vec<QueryEntry<Cow<'s, str>>>>,
    pub name: QueryEntry<Cow<'s, str>>,
    pub subtypes: QueryEntry<Vec<TypeTagQuery<'s>>>,
}

impl fmt::Display for TypeTagQuery<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let QueryEntry::Value(nss) = &self.namespace {
            if !nss.is_empty() {
                for (idx, ns) in nss.iter().enumerate() {
                    match ns {
                        QueryEntry::Value(val) => write!(f, "{}::", val)?,
                        QueryEntry::Any(p) => {
                            if self.name.is_any()
                                && self.subtypes.is_any()
                                && *p
                                && nss.len() == idx + 1
                            {
                                return write!(f, "**");
                            } else {
                                write!(f, "{}::", if *p { "**" } else { "*" })?
                            }
                        }
                    };
                }
            }

            write!(f, "{}", &self.name)?;
        } else {
            if let QueryEntry::Value(name) = &self.name {
                write!(f, "**::{}", name)?;
            } else {
                write!(f, "**")?;
            }
        };

        if let QueryEntry::Value(sts) = &self.subtypes {
            if !sts.is_empty() {
                write!(f, "<")?;

                for (idx, st) in sts.iter().enumerate() {
                    if idx != 0 {
                        write!(f, ", ")?;
                    }

                    write!(f, "{}", st)?;
                }

                write!(f, ">")?;
            }
        } else if !self.name.is_any() {
            write!(f, "<~>")?;
        }

        Ok(())
    }
}

impl<'a, 's: 'a> From<&'a TypeTagInfo<'s>> for TypeTagQuery<'s> {
    fn from(info: &'a TypeTagInfo<'s>) -> Self {
        Self {
            namespace: QueryEntry::Value(
                info.namespace
                    .iter()
                    .cloned()
                    .map(QueryEntry::Value)
                    .collect::<Vec<_>>(),
            ),
            name: QueryEntry::Value(info.name.clone()),
            subtypes: QueryEntry::Value(
                info.subtypes
                    .iter()
                    .map(TypeTagQuery::from)
                    .collect::<Vec<_>>(),
            ),
        }
    }
}

impl<'s> TypeTagQuery<'s> {
    pub fn any() -> Self {
        Self {
            namespace: QueryEntry::Any(false),
            name: QueryEntry::Any(false),
            subtypes: QueryEntry::Any(false),
        }
    }

    pub fn make_owned(&self) -> TypeTagQuery<'static> {
        TypeTagQuery {
            name: self.name.as_owned(),
            namespace: self
                .namespace
                .map(|x| x.iter().map(|x| x.as_owned()).collect()),
            subtypes: self
                .subtypes
                .map(|x| x.iter().map(|x| x.make_owned()).collect()),
        }
    }

    pub fn parse(s: &'s str) -> Option<TypeTagQuery<'s>> {
        let x = TypeTagQuery::parse_inner(s)?;

        if x.1.trim() != "" {
            return None;
        } else {
            Some(x.0)
        }
    }

    fn parse_inner(s: &'s str) -> Option<(TypeTagQuery<'s>, &str)> {
        let caps = QUERY_RE.captures(s)?;
        let ns = caps.get(1).map(|x| x.as_str().trim());
        let period = caps.get(2).map(|x| x.as_str().trim());
        let name = caps.get(3).map(|x| x.as_str().trim()).unwrap_or("*");
        let sts = caps.get(4).map(|x| x.as_str().trim());
        let rest = caps.get(5).map(|x| x.as_str()).unwrap_or("");

        let mut namespace = QueryEntry::Any(true);
        if let Some(ns) = ns {
            let mut ns_inner = Vec::new();
            for c in ns.split("::") {
                let c = c.trim();

                if !c.is_empty() {
                    ns_inner.push(match c {
                        "*" => QueryEntry::Any(false),
                        "**" => QueryEntry::Any(true),
                        c => QueryEntry::Value(Cow::Borrowed(c)),
                    })
                }
            }

            if !ns_inner.is_empty() || !period.is_some() {
                if let Some("**") = period {
                    ns_inner.push(QueryEntry::Any(true));
                }

                namespace = QueryEntry::Value(ns_inner);
            }
        }

        let name = if name == "*" {
            QueryEntry::Any(false)
        } else {
            QueryEntry::Value(Cow::Borrowed(name))
        };

        let subtypes = if let Some(mut sts) = sts {
            if sts != "~" {
                let mut subtypes = Vec::new();

                while let Some((tti, rest)) = TypeTagQuery::parse_inner(sts) {
                    subtypes.push(tti);
                    sts = rest.trim_start_matches(',');
                }

                QueryEntry::Value(subtypes)
            } else {
                QueryEntry::Any(true)
            }
        } else {
            QueryEntry::Value(Vec::new())
        };

        Some((
            Self {
                namespace,
                name,
                subtypes,
            },
            rest,
        ))
    }

    pub fn is_any(&self) -> bool {
        self.namespace.is_any() && self.name.is_any() && self.subtypes.is_any()
    }

    pub fn as_borrowed(&self) -> TypeTagQuery<'_> {
        let namespace = self
            .namespace
            .map(|x| x.iter().map(|x| x.as_borrowed()).collect());

        let name = self.name.as_borrowed();

        let subtypes = self
            .subtypes
            .map(|x| x.iter().map(|x| x.as_borrowed()).collect());

        TypeTagQuery {
            namespace,
            name,
            subtypes,
        }
    }

    pub fn reduce(&mut self) -> bool {
        if let QueryEntry::Value(sts) = &mut self.subtypes {
            for (idx, st) in sts.iter_mut().enumerate().rev() {
                if !st.is_any() && st.reduce() {
                    if st.is_any() && idx == 0 {
                        self.subtypes.make_any();
                    }

                    return true;
                }
            }
        }

        self.subtypes.make_any();

        if !self.name.is_any() {
            self.name.make_any();
            return true;
        }

        if let QueryEntry::Value(nss) = &mut self.namespace {
            for (idx, ns) in nss.iter_mut().enumerate().rev() {
                if !ns.is_any() {
                    ns.make_any_period();

                    if idx == 0 {
                        self.namespace.make_any();
                    }

                    return true;
                }
            }
        }

        false
    }

    fn reduced(&self) -> Option<TypeTagQuery<'_>> {
        let mut borrowed = self.as_borrowed();

        if borrowed.reduce() {
            return Some(borrowed);
        } else {
            return None;
        }
    }

    pub fn default_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }

    pub fn hashes(&self) -> impl Iterator<Item = u64> + '_ {
        iter::once_with(move || self.default_hash()).chain(iter::from_fn(move || {
            self.reduced().map(move |x| x.default_hash())
        }))
    }

    pub fn make_any(&mut self) {
        self.namespace.make_any();
        self.name.make_any();
        self.subtypes.make_any();
    }

    pub fn make_hashable(&mut self) -> bool {
        if let QueryEntry::Value(val) = &mut self.namespace {
            if let Some((idx, _)) = val.iter().enumerate().find(|(_, x)| x.is_any()) {
                if idx == 0 {
                    self.namespace.make_any();
                } else {
                    val.truncate(idx);
                    val.push(QueryEntry::Any(true));
                }

                self.name.make_any();
                self.subtypes.make_any();

                false
            } else {
                if self.name.is_any() {
                    self.subtypes.make_any();
                    false
                } else if let QueryEntry::Value(sts) = &mut self.subtypes {
                    let mut flag = false;

                    for st in sts {
                        if flag {
                            st.make_any();
                        } else if !st.make_hashable() {
                            flag = true
                        }
                    }

                    !flag
                } else {
                    false
                }
            }
        } else {
            self.name.make_any();
            self.subtypes.make_any();
            false
        }
    }

    pub fn test<'a, T: AsRef<TypeTagInfo<'a>>>(&self, tt: T) -> bool {
        let tt = tt.as_ref();

        if let QueryEntry::Value(ref val) = self.namespace {
            let mut q_iter = val.iter().peekable();
            let mut t_iter = tt.namespace.iter();

            loop {
                let qns = q_iter.next();

                match qns {
                    Some(QueryEntry::Value(ns)) => {
                        let tns = if let Some(tns) = t_iter.next() {
                            tns
                        } else {
                            return false;
                        };

                        if ns != tns {
                            return false;
                        }
                    }

                    Some(QueryEntry::Any(p)) => {
                        if !*p {
                            if t_iter.next().is_none() {
                                return false;
                            }
                        } else {
                            if let Some(qns_next) = q_iter.peek() {
                                match qns_next {
                                    QueryEntry::Value(qns_next) => {
                                        q_iter.next();

                                        while let Some(tns) = t_iter.next() {
                                            if tns == qns_next {
                                                continue;
                                            }
                                        }

                                        return false;
                                    }
                                    QueryEntry::Any(p) => {
                                        if *p {
                                            continue;
                                        } else {
                                            q_iter.next();

                                            if t_iter.next().is_none() {
                                                return false;
                                            }

                                            continue;
                                        }
                                    }
                                }
                            } else {
                                break;
                            }
                        }
                    }

                    None => {
                        let tns = t_iter.next();

                        if tns.is_some() {
                            return false;
                        } else {
                            break;
                        }
                    }
                }
            }
        }

        if let QueryEntry::Value(ref name) = self.name {
            if name != &tt.name {
                return false;
            }
        }

        if let QueryEntry::Value(ref sts) = self.subtypes {
            for (qsts, tsts) in iter::zip(sts.iter(), tt.subtypes.iter()) {
                if !qsts.test(tsts) {
                    return false;
                }
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::{TypeTagInfo, TypeTagQuery};

    #[test]
    fn test_ttq_display() {
        let tti = TypeTagInfo::parse("x::y::Z<a::b::C, d::e::F>").unwrap();
        let ttq = TypeTagQuery::from(&tti);

        assert_eq!(
            format!("{}", ttq),
            String::from("x::y::Z<a::b::C, d::e::F>")
        )
    }

    #[test]
    fn test_ttq_parse() {
        let ttq = TypeTagQuery::parse("*").unwrap();
        assert_eq!(format!("{}", ttq), String::from("*"));

        let ttq = TypeTagQuery::parse("**").unwrap();
        assert_eq!(format!("{}", ttq), String::from("**"));

        let ttq = TypeTagQuery::parse("Z<~>").unwrap();
        assert_eq!(format!("{}", ttq), String::from("Z<~>"));

        let ttq = TypeTagQuery::parse("**::Z").unwrap();
        assert_eq!(format!("{}", ttq), String::from("**::Z"));

        let ttq = TypeTagQuery::parse("*::*::*::*").unwrap();
        assert_eq!(format!("{}", ttq), String::from("*::*::*::*"));

        let ttq = TypeTagQuery::parse("*::**::*<*, *>").unwrap();
        assert_eq!(format!("{}", ttq), String::from("*::**::*<*, *>"));

        let ttq = TypeTagQuery::parse("x::y::Z<a::**::C, d::e::F>").unwrap();

        assert_eq!(
            format!("{}", ttq),
            String::from("x::y::Z<a::**::C, d::e::F>")
        );
    }

    #[test]
    fn test_ttq_test() {
        let tti = TypeTagInfo::parse("x::y::Z<a::b::C, d::e::f::G>").unwrap();
        let ttq = TypeTagQuery::from(&tti);
        assert!(ttq.test(&tti));

        assert!(!TypeTagQuery::parse("*").unwrap().test(&tti));
        assert!(TypeTagQuery::parse("**").unwrap().test(&tti));
        assert!(TypeTagQuery::parse("x::**").unwrap().test(&tti));
        assert!(TypeTagQuery::parse("x::y::**").unwrap().test(&tti));
        assert!(TypeTagQuery::parse("x::*::**").unwrap().test(&tti));
        assert!(TypeTagQuery::parse("x::y::*").unwrap().test(&tti));
        assert!(TypeTagQuery::parse("*::*::*").unwrap().test(&tti));
        assert!(TypeTagQuery::parse("*::**::**::**::**::**::Z")
            .unwrap()
            .test(&tti));
        assert!(!TypeTagQuery::parse("*::*::*::*").unwrap().test(&tti));
        assert!(TypeTagQuery::parse("x::**::Z<~>").unwrap().test(&tti));
        assert!(!TypeTagQuery::parse("x::**::y::Z<~>").unwrap().test(&tti));
        assert!(TypeTagQuery::parse("**::Z<~>").unwrap().test(&tti));
        assert!(TypeTagQuery::parse("x::y::Z<**, **>").unwrap().test(&tti));
        assert!(TypeTagQuery::parse("x::y::Z<**::C, **::G>")
            .unwrap()
            .test(&tti));
        assert!(TypeTagQuery::parse("x::y::Z<a::**::C, d::**::G>")
            .unwrap()
            .test(&tti));
    }

    #[test]
    fn test_ttq_make_shashable() {
        let mut ttq = TypeTagQuery::parse("x::y::Z<a::**::C, d::**::G>").unwrap();
        ttq.make_hashable();
        assert_eq!(format!("{}", ttq), String::from("x::y::Z<a::**, **>"));

        let mut ttq = TypeTagQuery::parse("x::y::Z<a::b::C, d::e::f::G>").unwrap();
        ttq.make_hashable();
        assert_eq!(
            format!("{}", ttq),
            String::from("x::y::Z<a::b::C, d::e::f::G>")
        );

        let mut ttq = TypeTagQuery::parse("x::*::Z<a::b::C, d::e::f::G>").unwrap();
        ttq.make_hashable();
        assert_eq!(format!("{}", ttq), String::from("x::**"));
    }

    #[test]
    fn test_ttq_reduce() {
        let tti = TypeTagInfo::parse("x::y::Z<a::b::C, d::e::F>").unwrap();
        let mut ttq = TypeTagQuery::from(&tti);

        ttq.reduce();
        assert_eq!(
            format!("{}", ttq),
            String::from("x::y::Z<a::b::C, d::e::*>")
        );

        assert!(ttq.reduce());
        assert_eq!(format!("{}", ttq), String::from("x::y::Z<a::b::C, d::**>"));

        assert!(ttq.reduce());
        assert_eq!(format!("{}", ttq), String::from("x::y::Z<a::b::C, **>"));

        assert!(ttq.reduce());
        assert_eq!(format!("{}", ttq), String::from("x::y::Z<a::b::*, **>"));

        assert!(ttq.reduce());
        assert_eq!(format!("{}", ttq), String::from("x::y::Z<a::**, **>"));

        assert!(ttq.reduce());
        assert_eq!(format!("{}", ttq), String::from("x::y::Z<~>"));

        assert!(ttq.reduce());
        assert_eq!(format!("{}", ttq), String::from("x::y::*"));

        assert!(ttq.reduce());
        assert_eq!(format!("{}", ttq), String::from("x::**"));

        assert!(ttq.reduce());
        assert_eq!(format!("{}", ttq), String::from("**"));

        assert!(!ttq.reduce());
    }
}
