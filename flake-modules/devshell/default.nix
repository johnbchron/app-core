localFlake: { ... }: {
  perSystem = { pkgs, rust-toolchain, ... }: let
    mkShell = pkgs.devshell.mkShell;
  in {
    devShells.default = mkShell {
      packages = with pkgs; [
        # rust dev toolchain (with RA), built from current nixpkgs
        (rust-toolchain.dev-toolchain pkgs)

        # dependencies for local rust builds
        pkg-config openssl gcc

        # tools
        cargo-nextest
      ];

      motd = "\n  Welcome to the {2}app-core{reset} dev shell. Run {1}menu{reset} for commands.\n";

      commands = [
        {
          name = "check";
          command = "nix flake check $@";
          help = "Runs nix flake checks.";
          category = "[nix actions]";
        }
        {
          name = "update-crate-graph";
          command = ''
            echo "building crate graph image"
            CRATE_GRAPH_IMAGE_PATH=$(nix build .#crate-graph-image --print-out-paths --no-link)
            echo "updating crate graph image in repo ($PRJ_ROOT/media/crate-graph.svg)"
            cp $CRATE_GRAPH_IMAGE_PATH/crate-graph.svg $PRJ_ROOT/media/crate-graph.svg --no-preserve=mode
          '';
          help = "Update the crate graph";
          category = "[repo actions]";
        }
      ];
    };
  };
}
