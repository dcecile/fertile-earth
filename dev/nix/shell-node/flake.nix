{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [
          (final: prev: {
            node-packages = prev.callPackage ./node-packages/default.nix {
              pkgs = pkgs;
              system = system;
              nodejs = pkgs.nodejs_20;
            };
          })
        ];
        pkgs = import nixpkgs { inherit system overlays; };
      in
        {
          devShells.default = pkgs.mkShell {
            buildInputs = [
              pkgs.nodejs_20
              pkgs.nodejs_20.pkgs.node2nix
              pkgs.node-packages.pnpm
            ];

            shellHook = ''
              echo "node $(node --version)"
              echo "pnpm v$(pnpm --version)"
            '';
          };
        }
    );
}
