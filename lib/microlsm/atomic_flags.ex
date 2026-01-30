defmodule Microlsm.AtomicFlags do
  @moduledoc false
  # Small module which provides implmenetation of binary flags on atomics

  @schema [
    :memtables,
    :gentables,
    :reading,
  ]

  indexed_schema = Enum.with_index(@schema, 1)

  @opaque t() :: :atomics.atomics_ref()

  @type key() :: :memtables | :gentables | :reading

  @compile {:inline, indexof: 1}
  for {key, index} <- indexed_schema do
    def indexof(unquote(key)), do: unquote(index)
  end

  @spec new() :: t()
  def new do
    :atomics.new(unquote(length(@schema)), signed: false)
  end

  @spec select(t(), key(), value0, value1) :: value0 | value1
  when value0: term(), value1: term()
  def select(atomics_ref, key, v0, v1) do
    case :atomics.get(atomics_ref, indexof(key)) do
      0 -> v0
      1 -> v1
    end
  end

  @spec order(t(), key(), value0, value1) :: {value0, value1} | {value1, value0}
  when value0: term(), value1: term()
  def order(atomics_ref, key, v0, v1) do
    case :atomics.get(atomics_ref, indexof(key)) do
      0 -> {v0, v1}
      1 -> {v1, v0}
    end
  end

  @spec swap(t(), key()) :: :ok
  def swap(atomics_ref, key) do
    index = indexof(key)
    old = :atomics.get(atomics_ref, index)
    new = 1 - old
    :ok = :atomics.compare_exchange(atomics_ref, index, old, new)
  end
end
