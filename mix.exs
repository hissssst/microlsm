defmodule Microlsm.MixProject do
  use Mix.Project

  @version "0.1.0"
  @description "An embedded key-value database"
  @source_url "https://github.com/hissssst/microlsm"

  def project do
    [
      app: :microlsm,
      elixirc_paths: elixirc_paths(Mix.env()),
      version: @version,
      elixir: "~> 1.17",
      description: @description,
      name: "Microlsm",
      start_permanent: Mix.env() == :prod,
      source_url: @source_url,
      deps:  [
        {:odcounter, "~> 2.0"},

        {:stream_data, "~> 1.2", only: [:test]},
        {:eflambe, "~> 0.3.2", only: [:dev, :test]},
        {:ex_doc, "~> 0.28", only: :dev, runtime: false},
        {:dialyxir, "~> 1.4.6", only: :dev, runtime: false}
      ],
      package: [
        description: @description,
        licenses: ["BSD-2-Clause"],
        files: [
          "lib",
          "mix.exs",
          "README.md",
          ".formatter.exs"
        ],
        maintainers: [
          "hissssst"
        ],
        links: %{
          GitHub: @source_url,
          Changelog: "#{@source_url}/blob/main/CHANGELOG.md"
        }
      ],
      docs: [
        source_ref: @version,
        main: "readme",
        extras: ["README.md", "CHANGELOG.md"]
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    []
  end
end
