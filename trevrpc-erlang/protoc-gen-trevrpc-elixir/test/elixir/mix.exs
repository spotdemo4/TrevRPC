# Compiles and runs the Elixir code protoc-gen-trevrpc-elixir generates from
# proto/, into gen/, with test.sh. The trevrpc application comes
# from ERL_LIBS, not Hex, so that the fixture builds offline.
defmodule Fixture.MixProject do
  use Mix.Project

  def project do
    [
      app: :fixture,
      version: "0.0.0",
      elixir: "~> 1.18",
      # The generated code, Elixir and gpb's Erlang, outside the formatter's
      # inputs, as the README suggests.
      elixirc_paths: ["gen"],
      erlc_paths: ["gen"],
      elixirc_options: [warnings_as_errors: true],
      deps: []
    ]
  end

  def application do
    [extra_applications: [:logger, :trevrpc]]
  end
end
