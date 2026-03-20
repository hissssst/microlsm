defmodule Microlsm.Memtable do
  @moduledoc false

  @type t :: :ets.table()

  @spec new() :: t()
  def new do
    :ets.new(:microlsm_memtable, [:ordered_set, :public])
  end

  @spec clear(t()) :: :ok
  def clear(table) do
    :ets.delete_all_objects(table)
    :ok
  end

  @spec delete(t()) :: :ok
  def delete(table) do
    true = :ets.delete(table)
    :ok
  end

  @spec length(t()) :: non_neg_integer()
  def length(table) do
    :ets.info(table, :size)
  end

  @spec empty?(t()) :: boolean()
  def empty?(table) do
    __MODULE__.length(table) == 0
  end

  @spec memory(t()) :: non_neg_integer()
  def memory(table) do
    :ets.info(table, :memory)
  end

  @spec stream(t(), pos_integer()) :: Enumerable.t()
  def stream(memtable, read_ahead) do
    selector = [{:"$1", [], [:"$1"]}]
    do_stream(memtable, selector, read_ahead)
  end

  @spec range_read(t(), term(), term(), pos_integer()) :: Enumerable.t()
  def range_read(memtable, left_key, right_key, read_ahead) do
    # Ex2ms.fun do {key, value} when key >= left_key and key <= right_key -> {key, value} end
    selector = [{{:"$1", :"$2"}, [{:andalso, {:>=, :"$1", left_key}, {:"=<", :"$1", right_key}}], [{{:"$1", :"$2"}}]}]
    do_stream(memtable, selector, read_ahead)
  end

  @spec read(t(), term()) :: {:ok, term()} | :error
  def read(memtable, key) do
    case :ets.lookup(memtable, key) do
      [{_, value}] -> {:ok, value}
      _ -> :error
    end
  end

  @spec write(t(), [{term(), term()}]) :: :ok
  def write(memtable, pairs) do
    :ets.insert(memtable, pairs)
    :ok
  end

  @spec write(t(), term(), term()) :: :ok
  def write(memtable, key, value) do
    :ets.insert(memtable, {key, value})
    :ok
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
