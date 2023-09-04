{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = { nixpkgs, flake-utils, rust-overlay, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };
      in
        {
          devShells.default = pkgs.mkShell {
            buildInputs = [
              pkgs.rust-bin.stable."1.57.0".default
            ];

            shellHook = ''
              rustc --version
              cargo --version
            '';
          };
        }
    );
}