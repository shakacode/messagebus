use core::fmt;
use std::{
    borrow::Cow,
    hash::{Hash, Hasher},
};

use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
    static ref INFO_RE: Regex = Regex::new(
        r"^((?:\s*[a-zA-Z_][a-zA-Z0-9_]*::)*)\s*([a-zA-Z_][a-zA-Z0-9_]*)(?:<(.*)>)?\s*(.*)"
    )
    .unwrap();
}

#[derive(Debug, Clone)]
pub struct TypeTagInfo<'s> {
    pub namespace: Vec<Cow<'s, str>>,
    pub name: Cow<'s, str>,
    pub subtypes: Vec<TypeTagInfo<'s>>,
}

impl Hash for TypeTagInfo<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for ns in &self.namespace {
            ns.as_ref().hash(state);
        }

        self.name.as_ref().hash(state);

        for st in &self.subtypes {
            st.hash(state);
        }
    }
}

impl fmt::Display for TypeTagInfo<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.namespace.is_empty() {
            for ns in &self.namespace {
                write!(f, "{}::", ns)?;
            }
        }

        write!(f, "{}", &self.name)?;

        if !self.subtypes.is_empty() {
            write!(f, "<")?;

            for (idx, st) in self.subtypes.iter().enumerate() {
                if idx != 0 {
                    write!(f, ", ")?;
                }

                write!(f, "{}", st)?;
            }
            write!(f, ">")?;
        }

        Ok(())
    }
}

impl<'s> TypeTagInfo<'s> {
    pub fn parse(s: &'s str) -> Option<TypeTagInfo<'s>> {
        Some(TypeTagInfo::parse_inner(s)?.0)
    }

    fn parse_inner(s: &'s str) -> Option<(TypeTagInfo<'s>, &str)> {
        let caps = INFO_RE.captures(s)?;
        let ns = caps.get(1).map(|x| x.as_str());
        let name = caps.get(2).map(|x| x.as_str())?;
        let sts = caps.get(3).map(|x| x.as_str());
        let rest = caps.get(4).map(|x| x.as_str()).unwrap_or("");

        let mut namespace = Vec::new();
        if let Some(ns) = ns {
            for c in ns.split("::") {
                if !c.is_empty() {
                    namespace.push(Cow::Borrowed(c.trim()));
                }
            }
        }

        let mut subtypes = Vec::new();
        if let Some(mut sts) = sts {
            while let Some((tti, rest)) = TypeTagInfo::parse_inner(sts.trim()) {
                subtypes.push(tti);
                sts = rest.trim_start_matches(',');
            }
        }

        Some((
            Self {
                namespace,
                name: Cow::Borrowed(name.trim()),
                subtypes,
            },
            rest,
        ))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_parse() {}
}
