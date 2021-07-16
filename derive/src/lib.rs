#![recursion_limit="128"]

extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::TokenTree;
use syn::{LitStr, Result, parenthesized};
use syn::{DeriveInput, parse_macro_input, punctuated::Punctuated, token::Comma};
use syn::parse::{Parse, ParseStream, Parser};
use quote::quote;

fn shared_part(_: &syn::Ident, has_shared: bool) -> proc_macro2::TokenStream {
    if has_shared {
        quote! {
            fn as_shared_ref(&self) -> std::option::Option<&dyn messagebus::SharedMessage> {Some(self)}
            fn as_shared_mut(&mut self) -> std::option::Option<&mut dyn messagebus::SharedMessage>{Some(self)}
            fn as_shared_boxed(self: std::boxed::Box<Self>) -> Option<std::boxed::Box<dyn messagebus::SharedMessage>>{Some(self)}
            fn as_shared_arc(self: std::sync::Arc<Self>) -> Option<std::sync::Arc<dyn messagebus::SharedMessage>>{Some(self)}
        }
    } else {
        quote! {
            fn as_shared_ref(&self) -> std::option::Option<&dyn messagebus::SharedMessage> {None}
            fn as_shared_mut(&mut self) -> std::option::Option<&mut dyn messagebus::SharedMessage>{None}
            fn as_shared_boxed(self: std::boxed::Box<Self>) -> Option<std::boxed::Box<dyn messagebus::SharedMessage>>{None}
            fn as_shared_arc(self: std::sync::Arc<Self>) -> Option<std::sync::Arc<dyn messagebus::SharedMessage>>{None}
        }
    }
}

fn clone_part(name: &syn::Ident, has_clone: bool) -> proc_macro2::TokenStream {
    if has_clone {
        quote! {
            fn try_clone_into(&self, into: &mut dyn core::any::Any) -> bool {
                let into = if let Some(inner) = into.downcast_mut::<Option<#name>>() {
                    inner
                } else {
                    return false;
                };
    
                into.replace(self.clone());
                true
            }
            fn try_clone_boxed(&self) -> std::option::Option<std::boxed::Box<dyn messagebus::Message>>{
                Some(Box::new(self.clone()))
            }
        }
    } else {
        quote! {
            fn try_clone_into(&self, into: &mut dyn core::any::Any) -> bool {false}
            fn try_clone_boxed(&self) -> std::option::Option<std::boxed::Box<dyn messagebus::Message>>{ None }
        }
    }
}

fn type_tag_part(name: &syn::Ident, type_tag: Option<LitStr>) -> proc_macro2::TokenStream {
    if let Some(type_tag) = type_tag {
        quote! {
            impl messagebus::TypeTagged for #name {
                fn type_tag_() -> messagebus::TypeTag { #type_tag.into() }
                fn type_tag(&self) -> messagebus::TypeTag {  #type_tag.into() }
                fn type_name(&self) -> std::borrow::Cow<str> {  #type_tag.into() }
            }
        }
    } else {
        quote! {
            impl messagebus::TypeTagged for #name {
                fn type_tag_() -> messagebus::TypeTag { std::any::type_name::<Self>().into() }
                fn type_tag(&self) -> messagebus::TypeTag { std::any::type_name::<Self>().into() }
                fn type_name(&self) -> std::borrow::Cow<str> { std::any::type_name::<Self>().into() }
            }
        }
    }
}

struct TypeTag {
    pub inner: syn::LitStr,
}

impl Parse for TypeTag {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut inner = None;
        let content;
        parenthesized!(content in input);
        let punctuated = Punctuated::<syn::LitStr, Comma>::parse_terminated(&content)?;

        for pair in punctuated.pairs() {
            inner = Some(pair.into_value());
            break;
        } 

        Ok(TypeTag { inner: inner.unwrap().to_owned() })
    }
}

#[derive(Default)]
struct Tags {
    has_clone: bool,
    has_shared: bool,
}

impl Tags {
    pub fn add(&mut self, other: Tags) {
        self.has_clone = self.has_clone || other.has_clone;
        self.has_shared = self.has_shared || other.has_shared;
    }
}

impl Parse for Tags {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut has_shared = false;
        let mut has_clone = false;
        
        let content;
        parenthesized!(content in input);
        let punctuated = Punctuated::<syn::Ident, Comma>::parse_terminated(&content)?;

        for pair in punctuated.pairs() {
            match pair.into_value().to_string().as_str() {
                "shared" => has_shared = true,
                "clone" => has_clone = true,
                _ => ()
            }
        } 

        Ok(Tags {
            has_clone,
            has_shared,
        })
    }
}

#[proc_macro_derive(Message, attributes(type_tag, message))]
pub fn derive_message(input: TokenStream) -> TokenStream {
    let mut tags = Tags::default();
    let mut type_tag = None;

    let ast: DeriveInput = syn::parse(input).unwrap();
    let name = &ast.ident;
    let attrs = ast.attrs;
    for attr in attrs {
        if let Some(i) = attr.path.get_ident() {
            match i.to_string().as_str() {
                "message" => {
                    let tt: Tags = syn::parse2(attr.tokens).unwrap();
                    tags.add(tt);
                }

                "type_tag" => {
                    let tt: TypeTag = syn::parse2(attr.tokens).unwrap();
                    type_tag = Some(tt.inner);
                }

                _ => ()
            }
        }
    }

    let type_tag_part = type_tag_part(name, type_tag);
    let shared_part = shared_part(name, false);
    let clone_part = clone_part(name, false);

    let tokens = quote! {
        #type_tag_part

        impl messagebus::Message for #name {
            fn as_any_ref(&self) -> &dyn core::any::Any {self}
            fn as_any_mut(&mut self) -> &mut dyn core::any::Any {self}
            fn as_any_boxed(self: std::boxed::Box<Self>) -> std::boxed::Box<dyn core::any::Any> {self}
            fn as_any_arc(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn core::any::Any> {self}

            #shared_part
            #clone_part
        }
    };

    tokens.into()
}

#[proc_macro_derive(Error, attributes(type_tag))]
pub fn derive_error(input: TokenStream) -> TokenStream {
    let mut type_tag = None;
    let ast: DeriveInput = syn::parse(input).unwrap();
    let name = &ast.ident;
    for attr in ast.attrs {
        if let Some(i) = attr.path.get_ident() {
            match i.to_string().as_str() {
                "type_tag" => {
                    let tt: TypeTag = syn::parse2(attr.tokens).unwrap();
                    type_tag = Some(tt.inner);
                }

                _ => ()
            }
        }
    }

    let type_tag_part = type_tag_part(name, type_tag);
    
    let tokens = quote! {
        #type_tag_part
    };

    tokens.into()
}