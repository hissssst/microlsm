defmodule Microlsm.Stats do
  @counters ~w[
    memtable_hit
    memtable_miss
    bounds_hit
    bounds_miss
    bloom_filter_hit
    bloom_filter_miss
    disktable_hit
    disktable_miss
    complete_miss
  ]a

  for {atom, int} <- Enum.with_index(@counters) do
    defp atoi(unquote(atom)), do: unquote(int + 1)
  end

  def init(name) do
    counters_ref = :counters.new(unquote(length(@counters)), [:write_concurrency])
    :persistent_term.put({__MODULE__, name}, counters_ref)
    :ok
  end

  defmacro bump(name, counter) when counter in @counters do
    index = atoi(counter)
    quote do
      case :persistent_term.get({unquote(__MODULE__), unquote(name)}, nil) do
        nil ->
          :ok

        counters_ref ->
          :counters.add(counters_ref, unquote(index), 1)
      end
    end
  end

  defmacro bump(_, other) do
    raise CompileError,
      file: __CALLER__.file,
      line: __CALLER__.line,
      message: "#{inspect(other)} is not an available counter"
  end

  @floor 3

  def print(name) do
    counters_ref = :persistent_term.get({__MODULE__, name})

    %{} = stats =
      Map.new(@counters, fn counter ->
        {counter, :counters.get(counters_ref, atoi(counter))}
      end)

    IO.puts "\nStats"

    memtable_miss_rate = Float.floor(100 * (stats.memtable_miss / (stats.memtable_miss + stats.memtable_hit)), @floor)
    IO.puts "Memtable miss rate              #{memtable_miss_rate} %"

    read_hit_rate = Float.floor((stats.disktable_hit + stats.disktable_miss) / (stats.memtable_miss - stats.complete_miss), @floor)
    IO.puts "Reads per disk hit              #{read_hit_rate} times"

    bloom_filter_miss_rate = Float.floor(100 * stats.disktable_miss / stats.bloom_filter_hit, @floor)
    IO.puts "Bloom filter miss rate          #{bloom_filter_miss_rate} %"

    read_hitmiss_rate = Float.floor(100 * (stats.disktable_hit + stats.disktable_miss) / stats.memtable_miss, @floor)
    IO.puts "Disk access per memtable miss   #{read_hitmiss_rate} %"

    IO.puts "\nRaw table"

    table =
      for {name, value} <- stats do
        name = String.pad_trailing(to_string(name), unquote(Enum.max(Enum.map(@counters, &String.length(to_string(&1)))) + 1))
        value = String.pad_leading(to_string(value), 12)
        [name, value, ?\n]
      end

    IO.puts table
  end
end
