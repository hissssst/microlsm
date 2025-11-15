defmodule Microlsm.Memtable do
  @moduledoc false

  def new do
    :ets.new(:microlsm_memtable, [:ordered_set, :public])
  end

  def clear(table) do
    :ets.delete_all_objects(table)
  end

  def size(table) do
    :ets.info(table, :size)
  end

  def memory(table) do
    :ets.info(table, :memory)
  end

  def stream(memtable, read_ahead) do
    selector = [{:"$1", [], [:"$1"]}]
    do_stream(memtable, selector, read_ahead)
  end

  def range_read(memtable, left_key, right_key, read_ahead) do
    # Ex2ms.fun do {key, value} when key >= left_key and key <= right_key -> {key, value} end
    selector = [{{:"$1", :"$2"}, [{:andalso, {:>=, :"$1", left_key}, {:"=<", :"$1", right_key}}], [{{:"$1", :"$2"}}]}]
    do_stream(memtable, selector, read_ahead)
  end

  def read(memtable, key) do
    case :ets.lookup(memtable, key) do
      [{_, value}] -> {:ok, value}
      _ -> :error
    end
  end

  def write(memtable, pairs) do
    :ets.insert(memtable, pairs)
  end

  defp do_stream(memtable, selector, read_ahead) do
    Stream.resource(
      fn -> :ets.select(memtable, selector, read_ahead) end,
      fn
        :"$end_of_table" ->
          {:halt, []}

        {pairs, continuation} ->
          {pairs, :ets.select(continuation)}
      end,
      fn _ -> [] end
    )
  end
end
