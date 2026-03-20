# Microlsm

Persistent embedded key-value storage for Elixir

## Features

1. Durable. Every write survives crashes

2. Efficient. Reading is faster than in `dets` and is comparable to RocksDB

3. Embedded. Has native support for all Elixir terms

## Installation

```elixir
def deps do
  [
    {:microlsm, "~> 0.1"}
  ]
end
```

## Usage

1. Add Microlsm to your supervision tree and provide an atom name and
a directory to store the data in. This directory will
be used only by this specific Microlsm table
```elixir
defmodule MyProject.Application do
  use Application

  def start(_type, _args) do
    children = [
      {Microlsm, name: :my_table, data_dir: ""}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
```

2. Read and write
```elixir
Microlsm.write(:my_table, :key, value)
{:ok, ^value} = Microlsm.read(:my_table, :key)
```
