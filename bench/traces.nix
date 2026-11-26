# The link-capacity traces that `--netem` profiles replay with `trace NAME`, as NAME.up and
# NAME.down in one directory. They are Mahimahi's (https://github.com/ravinet/mahimahi), recorded
# with the Saturator tool of Winstein et al., "Stochastic Forecasts Achieve High Throughput and Low
# Delay over Cellular Networks" (NSDI 2013), and fetched at a fixed commit rather than copied into
# this repository: Mahimahi is GPL-3.0.
{
  fetchurl,
  linkFarm,
}:
let
  commit = "f1346c38c415abfc98626dd82450f49a57033603";
  trace = name: hash: {
    inherit name;
    path = fetchurl {
      url = "https://raw.githubusercontent.com/ravinet/mahimahi/${commit}/traces/${name}";
      inherit hash;
    };
  };
in
linkFarm "trevrpc-bench-traces" [
  (trace "Verizon-LTE-driving.up" "sha256-ZphA9iaWbXSQonYp1CJ+sqhm1A8QSmIXxTs/S1ogfR8=")
  (trace "Verizon-LTE-driving.down" "sha256-efGGC3sZVJbX04jQkO2z6qnOy737SJv0pac9ldjC584=")
]
