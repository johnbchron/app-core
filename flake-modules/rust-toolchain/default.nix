localFlake: { inputs, ... }: {
  perSystem = { pkgs, ... }: let
    # build the CI and dev toolchains
    toolchain = p: p.rust-bin.selectLatestNightlyWith (toolchain: toolchain.minimal.override {
      extensions = [ "rustfmt" "clippy" ];
    });
    dev-toolchain = p: p.rust-bin.selectLatestNightlyWith (toolchain: toolchain.default.override {
      extensions = [ "rust-src" "rust-analyzer" ];
    });

    # configure crane to use the CI toolchain
    craneLib = (inputs.crane.mkLib pkgs).overrideToolchain toolchain;
  in {
    config._module.args.rust-toolchain = {
      inherit toolchain dev-toolchain craneLib;
    };
  };
}
