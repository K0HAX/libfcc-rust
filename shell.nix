{ pkgs ? import <nixpkgs> { } }:
pkgs.mkShell {
    # Get dependencies from the main package
    inputsFrom = [ (pkgs.callPackage ./default.nix { }) ];
    # Additional tooling
    buildInputs = with pkgs; [
        rust-analyzer   # LSP Server
        rustfmt         # Formatter
        clippy          # Linter
    ];
}

#let
  #pkgs = import <nixpkgs> { config.allowUnfree = true; };
#in
#pkgs.mkShellNoCC {
  #PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig";
  #packages = with pkgs; [
    #gcc
    #cargo
    #rustfmt
    #sqlite
    #pkg-config
  #];
#}


