//! Assorted helper functions used throughout the code.

use crate::Session;

pub(crate) trait ProtectedInner<T> {
    fn inner(&self) -> T;
}

/// Interconvert between external and internal representation.
/// We can freely do this within this crate, but must not allow
/// our clients to do this.
pub(crate) trait Protected<T>: ProtectedInner<T> {
    fn build(inner: T) -> Self;
}

pub(crate) trait ProtectedWithSession<T>: ProtectedInner<T> {
    fn build(inner: T, session: Session) -> Self;
    fn session(&self) -> &Session;
}

/// Enhance a nullary enum as follows:
///
/// * `Display` / `to_string`
/// * `FromStr` / `parse` (with just a simple `String` as error type)
/// * Interconvert with another type (via `Protected`).
/// * `variants` yields an array of all variants.
///
/// Only works for nullary enums, i.e., ones where no variants have any arguments.
///
/// # Syntax
///
/// ```ignore
/// enhance_nullary_enum(ThisEnum, ThatEnum, {
///     (ThisVariant1, ThatVariant1, "StringName1"),
///     (ThisVariant2, ThatVariant2, "StringName2"),
///     ...
/// }, omit { ThatVariantOmit1, ThatVariantOmit2 });
/// ```
/// where
///
/// * `ThisEnum` is the name of the type being enhanced.
/// * `ThatEnum` is the name of the inner type wrapped by `ThisEnum`.
/// * Then all variants of `ThisEnum` are listed:
///   * `ThisVariant`i is the name of the variant.
///   * `ThatVariant`i is the name of the corresponding variant of `ThatEnum`.
///   * `StringName`i is the desired string representation of the enum for parsing and printing.
/// * The `omit` section is optional; any variants of `ThatEnum` listed here
///   cause `Protected::build` to panic.
///

// We attempted to use the `macro-attr` crate to achieve this more naturally, but sadly
// identifier-concatenation functionality is not yet available in stable Rust
// per https://github.com/rust-lang/rust/issues/29599. We really don't want to use the verbose
// identifiers (e.g., `CASS_CONSISTENCY_ANY`), so without concatenation we are forced to
// provide an explicit list of identifiers. However with `macro-attr` there's nowhere to
// hang them; we can't add them in a custom attribute because custom attributes are unstable.
// In the end the best approach is just the direct one, as exemplified here.

macro_rules! enhance_nullary_enum {
    ( $this_name:ident, $that_name: ident, {
        $( ($this:ident, $that:ident, $name:expr), )*
        } $( , omit { $( $not_that:ident ),* } )* ) => {
        impl ::std::fmt::Display for $this_name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::result::Result<(), ::std::fmt::Error> {
                write!(f, "{}", match *self {
                    $( $this_name::$this => $name, )*
                })
            }
        }

        impl ::std::str::FromStr for $this_name {
            type Err = String;

            fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
                match s {
                    $( $name => Ok($this_name::$this), )*
                    _ => Err(format!("Unrecognized {}: {}", stringify!($this_name), s)),
                }
            }
        }

        impl $crate::cassandra::util::ProtectedInner<$that_name> for $this_name {
            fn inner(&self) -> $that_name {
                match *self {
                    $( $this_name::$this => $that_name::$that ),*
                }
            }
        }

        impl $crate::cassandra::util::Protected<$that_name> for $this_name {
            fn build(inner: $that_name) -> Self {
                match inner {
                    $( $that_name::$that => $this_name::$this, )*
                    $($( $that_name::$not_that => panic!(stringify!(Unexpected variant $that_name::$not_that)), )*)*
                }
            }
        }

        impl $this_name {
            /// List all the possible values of this enumeration.
            pub fn variants() -> &'static [$this_name] {
                // Nasty trick to calculate the length of the iteration - we must mention
                // a variable inside the layer, even though we never actually use it.
                static VARIANTS: [ $this_name; 0 $( + ($this_name::$this, 1).1 )* ] =
                  [ $( $this_name::$this ),* ];
                &VARIANTS
            }
        }
    };
}
