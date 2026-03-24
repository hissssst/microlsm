defmodule Microlsm.Stats do
  @moduledoc false

  defmacrop floorwrap(code) do
    quote do
      try do
        unquote(code)
      else
        result -> Float.floor(result, 3)
      rescue
        _ -> "undefined"
      end
    end
  end

  def print(name) do
    stats = ODCounter.to_map(Microlsm, name)

    IO.puts "\nStats"

    memtable_miss_rate = floorwrap(100 * ((stats.memtable_miss - stats.complete_miss) / (stats.memtable_miss + stats.memtable_hit)))
    IO.puts "Memtable miss rate              #{memtable_miss_rate} %"

    read_hit_rate = floorwrap((stats.disktable_hit + stats.disktable_miss) / (stats.memtable_miss - stats.complete_miss))
    IO.puts "Reads per disk hit              #{read_hit_rate} times"

    bloom_filter_miss_rate = floorwrap(100 * stats.disktable_miss / stats.bloom_filter_hit)
    IO.puts "Bloom filter miss rate          #{bloom_filter_miss_rate} %"

    read_hitmiss_rate = floorwrap(100 * (stats.disktable_hit + stats.disktable_miss) / stats.memtable_miss)
    IO.puts "Disk access per memtable miss   #{read_hitmiss_rate} %"

    IO.puts "\nRaw table"

    stats = Map.merge(stats, Microlsm.Fs.stats())

    table =
      for {name, value} <- stats do
        name = String.pad_trailing(to_string(name), 20)
        value = String.pad_leading(to_string(value), 12)
        [name, value, ?\n]
      end

    IO.puts table
  end
end
