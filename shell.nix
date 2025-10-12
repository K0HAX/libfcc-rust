let
  pkgs = import <nixpkgs> { config.allowUnfree = true; };
in
pkgs.mkShellNoCC {
  PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig";
  packages = with pkgs; [
    gcc
    cargo
    rustfmt
    sqlite
    pkg-config
  ];
}


