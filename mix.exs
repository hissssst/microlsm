defmodule Microlsm.MixProject do
  use Mix.Project

  def project do
    [
      app: :microlsm,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    []
  end

  defp deps do
    [
      {:nimble_pool, "~> 1.1.0"},
      {:eflambe, "~> 0.3.2", only: [:dev, :test]},
      {:dialyxir, "~> 1.4.6", only: :dev, runtime: false}
    ]
  end
end
