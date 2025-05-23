//! Provides a proc-macro for rendering logos to ASCII-art strings.
//!
//! This crate is really very simple. It provides a single macro, `ascii_art`,
//! which takes a single argument: a string literal representing the path to an
//! image file. The macro reads the image file, decodes it, and converts it to
//! an ASCII-art string using the `artem` crate. The ASCII-art string
//! is then printed to stderr, plus a newline.
//!
//! # Example
//!
//! ```rust
//! use art::ascii_art;
//!
//! ascii_art!("../../media/ascii_logo.png");
//! ```

extern crate proc_macro;

use std::{
  fs::File,
  io::{Cursor, Read},
  num::NonZeroU32,
  path::{Path, PathBuf},
  str::FromStr,
};

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, LitStr};

/// Prints ASCII-art decoded from the provided image file.
#[proc_macro]
pub fn ascii_art(input: TokenStream) -> TokenStream {
  // Parse the input token stream as a string literal
  let input = parse_macro_input!(input as LitStr);
  let path = PathBuf::from_str(&input.value()).expect("Invalid path");
  let root = std::env::var("CARGO_MANIFEST_DIR").unwrap_or(".".into());
  let full_path = Path::new(&root).join(&path);

  // Read the image file
  let mut file = File::open(&full_path)
    .unwrap_or_else(|_| panic!("failed to open image file: {full_path:?}"));
  let mut buffer = Vec::new();
  file
    .read_to_end(&mut buffer)
    .expect("failed to read image file");

  // Decode the image
  let image = image::ImageReader::new(Cursor::new(buffer))
    .with_guessed_format()
    .expect("failed to guess image format")
    .decode()
    .expect("failed to decode image");

  // Convert to ASCII art
  let config = artem::config::ConfigBuilder::new()
    .target_size(NonZeroU32::new(80).unwrap())
    // .characters("█▓▒░ ".to_string())
    .characters("%0Oo. ".to_string())
    .build();
  let ascii_art = artem::convert(image, &config);

  // Generate the ASCII art string as a static literal
  let output = quote! {
      eprintln!(#ascii_art);
  };

  TokenStream::from(output)
}
