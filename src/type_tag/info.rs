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

impl<'s> AsRef<TypeTagInfo<'s>> for &'_ TypeTagInfo<'s> {
    fn as_ref(&self) -> &TypeTagInfo<'s> {
        &*self
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

    use core::fmt;

    use regex::Regex;

    type ParseResult<'a, Output> = Result<(&'a str, Output), &'a str>;
    pub trait Parser<'a> {
        type Output;

        fn parse(&self, input: &'a str) -> ParseResult<'a, Self::Output>;
    }

    impl<'a, F, T> Parser<'a> for F
    where
        F: Fn(&'a str) -> ParseResult<T>,
    {
        type Output = T;

        fn parse(&self, input: &'a str) -> ParseResult<'a, Self::Output> {
            self(input)
        }
    }

    impl<'a> Parser<'a> for &'static str {
        type Output = &'a str;

        fn parse(&self, input: &'a str) -> ParseResult<'a, Self::Output> {
            match input.get(0..self.len()) {
                Some(actual) if actual == *self => {
                    Ok((&input[self.len()..], &input[0..self.len()]))
                }
                _ => Err(input),
            }
        }
    }

    pub fn pattern<'a>(regex: &'a Regex) -> impl Parser<'a, Output = &'a str> {
        move |input: &'a str| match regex.find(input) {
            Some(match_) if match_.start() == 0 => {
                Ok((&input[match_.end()..], &input[0..match_.end()]))
            }
            _ => Err(input),
        }
    }

    pub fn take_until<'a, F>(pattern: F) -> impl Parser<'a, Output = &'a str>
    where
        F: Fn(char) -> bool,
    {
        move |input: &'a str| match input.find(&pattern) {
            Some(index) if index != 0 => Ok((&input[index..], &input[0..index])),
            _ => Err(input),
        }
    }

    #[macro_export]
    macro_rules! seq {
        ($($parsers: expr),+ $(,)?) => {
            ($($parsers),+)
        }
    }

    macro_rules! seq_impl {
        ($($parser:ident),+) => {
            #[allow(non_snake_case)]
            impl<'a, $($parser),+> Parser<'a> for ($($parser),+)
            where
                $($parser: Parser<'a>),+
            {
                type Output = ($($parser::Output),+);

                fn parse(&self, input: &'a str) -> ParseResult<'a, Self::Output> {
                    let ($($parser),+) = self;
                    seq_body_impl!(input, input, $($parser),+ ; )
                }
            }
        }
    }

    macro_rules! seq_body_impl {
        ($input:expr, $next_input:expr, $head:ident, $($tail:ident),+ ; $(,)? $($acc:ident),*) => {
            match $head.parse($next_input) {
                Ok((next_input, $head)) => seq_body_impl!($input, next_input, $($tail),+ ; $($acc),*, $head),
                Err(_) => Err($input),
            }
        };
        ($input:expr, $next_input:expr, $last:ident ; $(,)? $($acc:ident),*) => {
            match $last.parse($next_input) {
                Ok((next_input, last)) => Ok((next_input, ($($acc),+, last))),
                Err(_) => Err($input),
            }
        }
    }

    seq_impl!(A, B);
    seq_impl!(A, B, C);
    seq_impl!(A, B, C, D);
    seq_impl!(A, B, C, D, E);
    seq_impl!(A, B, C, D, E, F);
    seq_impl!(A, B, C, D, E, F, G);
    seq_impl!(A, B, C, D, E, F, G, H);
    seq_impl!(A, B, C, D, E, F, G, H, I);
    seq_impl!(A, B, C, D, E, F, G, H, I, J);

    #[macro_export]
    macro_rules! choice {
        ($parser: expr $(,)?) => {
            $parser
        };
        ($parser: expr, $($rest: expr),+ $(,)?) => {
            or($parser, choice!($($rest),+))
        }
    }

    pub fn map<'a, P, F, T>(parser: P, map_fn: F) -> impl Parser<'a, Output = T>
    where
        P: Parser<'a>,
        F: Fn(P::Output) -> T,
    {
        move |input| {
            parser
                .parse(input)
                .map(|(next_input, result)| (next_input, map_fn(result)))
        }
    }

    pub fn or<'a, P1, P2, T>(parser1: P1, parser2: P2) -> impl Parser<'a, Output = T>
    where
        P1: Parser<'a, Output = T>,
        P2: Parser<'a, Output = T>,
    {
        move |input| match parser1.parse(input) {
            ok @ Ok(_) => ok,
            Err(_) => parser2.parse(input),
        }
    }

    pub fn optional<'a, P, T>(parser: P) -> impl Parser<'a, Output = Option<T>>
    where
        P: Parser<'a, Output = T>,
    {
        move |input| match parser.parse(input) {
            Ok((next_input, value)) => Ok((next_input, Some(value))),
            Err(_) => Ok((input, None)),
        }
    }

    pub fn ws<'a, P, T>(parser: P) -> impl Parser<'a, Output = T>
    where
        P: Parser<'a, Output = T>,
    {
        // left(
        move |input: &'a str| parser.parse(input.trim()) //,
                                                         // pattern(&WS_PAT),
                                                         // )
    }

    pub fn left<'a, L, R, T>(left: L, right: R) -> impl Parser<'a, Output = T>
    where
        L: Parser<'a, Output = T>,
        R: Parser<'a>,
    {
        map(seq!(left, right), |(left_value, _)| left_value)
    }

    pub fn right<'a, L, R, T>(left: L, right: R) -> impl Parser<'a, Output = T>
    where
        L: Parser<'a>,
        R: Parser<'a, Output = T>,
    {
        map(seq!(left, right), |(_, right_value)| right_value)
    }

    pub fn zero_or_more<'a, P, T>(parser: P) -> impl Parser<'a, Output = Vec<T>>
    where
        P: Parser<'a, Output = T>,
    {
        move |mut input| {
            let mut values = Vec::new();

            while let Ok((next_input, value)) = parser.parse(input) {
                input = next_input;
                values.push(value);
            }

            Ok((input, values))
        }
    }

    pub fn sep<'a, P, S, T>(parser: P, separator: S) -> impl Parser<'a, Output = Vec<T>>
    where
        P: Parser<'a, Output = T>,
        S: Parser<'a>,
    {
        move |mut input| {
            let mut values = Vec::new();

            match parser.parse(input) {
                Ok((next_input, value)) => {
                    input = next_input;
                    values.push(value);
                }
                Err(err) => return Err(err),
            }

            loop {
                match separator.parse(input) {
                    Ok((next_input, _)) => input = next_input,
                    Err(_) => break,
                }

                match parser.parse(input) {
                    Ok((next_input, value)) => {
                        input = next_input;
                        values.push(value);
                    }
                    Err(_) => break,
                }
            }

            Ok((input, values))
        }
    }

    #[derive(Debug, Clone)]
    pub enum MaybeAny<T> {
        Some(T),
        Placeholder,
        Period,
    }

    #[derive(Debug, Clone)]
    pub struct TagTypeDef<'a> {
        namespace: Vec<MaybeAny<&'a str>>,
        name: Option<&'a str>,
        subtypes: Option<MaybeAny<Vec<TagTypeDef<'a>>>>,
    }

    impl<'a> TagTypeDef<'a> {
        fn any() -> TagTypeDef<'a> {
            TagTypeDef {
                namespace: vec![MaybeAny::Period],
                name: None,
                subtypes: Some(MaybeAny::Period),
            }
        }
    }

    impl<'a> fmt::Display for TagTypeDef<'a> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            if self.namespace.len() == 1
                && matches!(self.namespace[0], MaybeAny::Period)
                && self.name.is_none()
            // && self
            //     .subtypes
            //     .map(|st| matches!(st, MaybeAny::Period))
            //     .unwrap_or(false)
            {
                write!(f, "~")?
            } else {
                for p in &self.namespace {
                    match p {
                        MaybeAny::Some(s) => write!(f, "{}::", s)?,
                        MaybeAny::Placeholder => write!(f, "*::")?,
                        MaybeAny::Period => write!(f, "**::")?,
                    }
                }

                match &self.name {
                    Option::Some(s) => write!(f, "{}", s)?,
                    Option::None => write!(f, "*")?,
                }

                // if let Some(subtypes) = &self.subtypes {
                //     for (i, s) in subtypes.iter().enumerate() {
                //         if i != 0 {
                //             write!(f, ", ")?;
                //         }

                //         match s {
                //             MaybeAny::Some(s) => write!(f, "{}", s)?,
                //             MaybeAny::Placeholder => write!(f, "~")?,
                //             MaybeAny::Period => write!(f, "...")?,
                //         }
                //     }
                // }
            }

            Ok(())
        }
    }

    lazy_static::lazy_static! {
        static ref IDENT_PAT: Regex = Regex::new(r"^[_a-zA-Z][_a-zA-Z0-9]*").unwrap();
        static ref WS_PAT: Regex = Regex::new(r"^\s*").unwrap();
    }

    // pub fn parse_type<'a>() -> impl Parser<'a, Output = TagTypeDef<'a>> {
    //     let subtypes_inner = move |s| {
    //         sep(
    //             ws(or(map("...", |_| MaybeAny::Period), parse_type())),
    //             ws(","),
    //         )
    //         .parse(s)
    //     };

    //     let subtypes = left(right(ws("<"), ws(subtypes_inner)), ws(">"));
    //     let ns = zero_or_more(left(
    //         ws(or(
    //             map("**", |_| MaybeAny::Period),
    //             or(
    //                 map("*", |_| MaybeAny::Placeholder),
    //                 map(pattern(&IDENT_PAT), MaybeAny::Some),
    //             ),
    //         )),
    //         ws("::"),
    //     ));
    //     let name = ws(pattern(&IDENT_PAT));

    //     ws(or(
    //         map("~", |_| TagTypeDef::any()),
    //         map(
    //             seq!(ns, name, optional(subtypes)),
    //             |(ns, name, subtypes)| TagTypeDef {
    //                 namespace: ns,
    //                 name,
    //                 subtypes,
    //             },
    //         ),
    //     ))
    // }

    #[test]
    fn test_parse() {
        // println!(
        //     "{}",
        //     parse_type()
        //         .parse("api    ::    *    ::    Name    <   ~    , X<~>, ...    >   ")
        //         .unwrap()
        //         .1
        // );
    }
}
