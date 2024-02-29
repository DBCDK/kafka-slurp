{ nixpkgs ? import ./nixpkgs.nix, pkgs ? import nixpkgs { } }:
let
  python = pkgs.python3.withPackages (ps: with ps; [ kafka-python ]);
  make-test = import "${nixpkgs}/nixos/tests/make-test-python.nix";
  test = make-test {
    name = "kafka-slurp-test";
    nodes = {
      slurp = {
        environment.systemPackages = [ pkgs.kcat python ];
        services.zookeeper.enable = true;
        services.apache-kafka.enable = true;
        systemd.services.slurp = {
          # --record-limit 1, we only check a single file
          script = ''
            ${
              ./kafka-slurp.py
            } --brokers localhost:9092 --topic testtopic --data-dir /root/out --record-limit 1
          '';
          path = [ pkgs.lzma python ];
        };
      };
    };
    skipLint = true;
    testScript = ''
      start_all()
      slurp.wait_for_unit("zookeeper.service")
      slurp.wait_for_unit("apache-kafka.service")
      slurp.wait_until_succeeds(
          "${pkgs.apacheKafka}/bin/kafka-topics.sh --create "
          + "--bootstrap-server localhost:9092 --partitions 1 "
          + "--replication-factor 1 --topic testtopic"
      )

      slurp.execute(
        "systemctl start slurp"
      )      

      # Need two messages or kafka-slurp will spend time resetting onset
      slurp.wait_until_succeeds(
          "echo testmessage1 | ${pkgs.kcat}/bin/kcat -b localhost:9092 -t testtopic"
      )
      slurp.wait_until_succeeds(
          "echo testmessage2 | ${pkgs.kcat}/bin/kcat -b localhost:9092 -t testtopic"
      )

      slurp.wait_for_file(
        "/root/out/testtopic/0/0-0.jsonl.xz"
      )
    '';
  };
in { inherit test; }
