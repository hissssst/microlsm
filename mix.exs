defmodule Microlsm.MixProject do
  use Mix.Project

  def project do
    [
      app: :microlsm,
      elixirc_paths: elixirc_paths(Mix.env()),
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps:  [
        {:odcounter, "~> 2.0"},

        {:stream_data, "~> 1.2", only: [:test]},
        {:eflambe, "~> 0.3.2", only: [:dev, :test]},
        {:dialyxir, "~> 1.4.6", only: :dev, runtime: false}
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    []
  end
end
