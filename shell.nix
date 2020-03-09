{ pkgs ? import <nixpkgs> {} }:
pkgs.mkShell {
  name = "kafka-slurp";
  buildInputs = (with pkgs; [
    lzma
  ]) ++ (with pkgs.python37Packages; [
    kafka-python
  ]);
}
