{
  description = "Development shell for the prdb Rust and TinyCC toolchain";

  inputs.nixpkgs.url =
    "github:NixOS/nixpkgs/718f670e845328e9c2cecdd832835712b2cbd065";

  outputs = { nixpkgs, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
    in {
      devShells.${system}.default = pkgs.mkShell {
        packages = with pkgs; [
          rustc
          cargo
          clippy
          rustfmt
          tinycc
          gcc
          binutils
          patchelf
          gnumake
          python3
        ];
      };
    };
}
