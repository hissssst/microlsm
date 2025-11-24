defmodule Microlsm do
  @moduledoc """
  Microlsm is a tiny and simple LSM Key-Value storage
  """

  use GenServer

  alias Microlsm.BloomFilter
  alias Microlsm.Disktable
  alias Microlsm.Memtable
  alias Microlsm.Wal

  import :erlang, only: [term_to_binary: 1, binary_to_term: 1, element: 2, garbage_collect: 0]

  @call_timeout 15_000

  @type key :: any()

  @type value :: any()

  @type t :: atom()

  @type start_option ::
          {:name, t()}
          | {:data_dir, Path.t()}
          | {:threshold, bytes :: pos_integer()}
          | {:block_count, pos_integer()}
          | {:max_batch_length, pos_integer()}

  @type batch_operation ::
          {:delete, key()}
          | {:write, key(), value()}

  ## Public API

  @spec start_link([start_option()]) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec write(t(), key(), value()) :: :ok
  def write(name, key, value) do
    GenServer.call(name, {:write, key, {value}}, @call_timeout)
  end

  @spec async_write(t(), key(), value()) :: :ok
  def async_write(name, key, value) do
    GenServer.cast(name, {:async_write, key, {value}})
    :ok
  end

  @spec delete(t(), key()) :: :ok
  def delete(name, key) do
    GenServer.call(name, {:write, key, {}}, @call_timeout)
  end

  @spec async_delete(t(), key()) :: :ok
  def async_delete(name, key) do
    GenServer.cast(name, {:async_write, key, {}})
    :ok
  end

  @spec batch(t(), [batch_operation()]) :: :ok
  def batch(name, batch) do
    batch =
      Enum.map(batch, fn
        {:write, key, value} -> {key, {value}}
        {:delete, key} -> {key, {}}
      end)

    GenServer.call(name, {:batch, batch}, @call_timeout)
  end

  @spec read(t(), key()) :: {:ok, value()} | :error
  def read(name, key) do
    maybe_raise_when_no_table(name)
    {memtable, indices_table} = get_memtable_and_indices_table(name)
    indices = :ets.tab2list(indices_table)

    result =
      with :error <- Memtable.read(memtable, key) do
        disktable_read(indices, key)
      end

    case result do
      {:ok, {value}} -> {:ok, value}
      _ -> :error
    end
  end

  defp maybe_raise_when_no_table(name) do
    with nil <- Process.whereis(name) do
      raise ArgumentError, "No such table #{name}"
    end
  end

  @spec all(t()) :: Enumerable.t({key(), value()})
  def all(name) do
    maybe_raise_when_no_table(name)
    memtable_read_ahead = 1000

    {memtable, index_fds} = prepare_stream_read(name)
    memtable_stream = Memtable.stream(memtable, memtable_read_ahead)

    stream =
      index_fds
      |> Enum.reduce(memtable_stream, fn {index, fd}, stream ->
        disk_stream = Disktable.stream(fd, index)
        Microlsm.Stream.merge_streams(stream, disk_stream)
      end)
      |> Stream.flat_map(fn
        {key, binary} when is_binary(binary) ->
          case binary_to_term(binary) do
            {} -> []
            {value} -> [{key, value}]
          end

        {key, {value}} ->
          [{key, value}]

        {_, {}} ->
          []
      end)

    fds = for {_index, fd} <- index_fds, do: fd

    Microlsm.Stream.after_hook(stream, fn ->
      Enum.each(fds, &:prim_file.close/1)
    end)
  end

  @doc """
  Ranges must be sorted
  """
  @spec ranges_read(t(), [{key(), key()}]) :: Enumerable.t({key(), value()})
  def ranges_read(name, [{left_key, right_key} | ranges]) do
    Enum.reduce(ranges, range_read(name, left_key, right_key), fn {left_key, right_key}, stream ->
      range_stream = range_read(name, left_key, right_key)
      Stream.concat(stream, range_stream)
    end)
  end

  @spec range_read(t(), key(), key()) :: Enumerable.t({key(), value()})
  def range_read(name, left_key, right_key) when left_key <= right_key do
    maybe_raise_when_no_table(name)
    memtable_read_ahead = 1000

    {memtable, index_fds} = prepare_stream_read(name)
    memtable_stream = Memtable.range_read(memtable, left_key, right_key, memtable_read_ahead)

    stream =
      index_fds
      |> Enum.reduce(memtable_stream, fn {index, fd}, stream ->
        disk_stream = Disktable.range_stream(fd, index, left_key, right_key)
        Microlsm.Stream.merge_streams(stream, disk_stream)
      end)
      |> Stream.flat_map(fn
        {key, binary} when is_binary(binary) ->
          case binary_to_term(binary) do
            {} -> []
            {value} -> [{key, value}]
          end

        {key, {value}} ->
          [{key, value}]

        {_, {}} ->
          []
      end)

    fds = for {_index, fd} <- index_fds, do: fd

    Microlsm.Stream.after_hook(stream, fn ->
      Enum.each(fds, &:prim_file.close/1)
    end)
  end

  defp prepare_stream_read(name) do
    {memtable, indices_table} = :persistent_term.get({__MODULE__, name})
    indices = :ets.tab2list(indices_table)
    index_fds =
      for {_generation, _bloom_filter, index, filename} <- indices do
        {:ok, fd} = :prim_file.open(filename, [:read])
        {index, fd}
      end

    {memtable, index_fds}
  end

  defp disktable_read([], _key), do: :error

  defp disktable_read([{_generation, bloom_filter, index, filename} | rest], key) do
    case BloomFilter.check(bloom_filter, key) do
      :no ->
        disktable_read(rest, key)

      :maybe ->
        {:ok, fd} = :prim_file.open(filename, [:read])

        try do
          Disktable.find_value(fd, index, key)
        else
          {:ok, value} -> {:ok, value}
          {:error, _} -> disktable_read(rest, key)
        after
          :prim_file.close(fd)
        end
    end
  end

  ## Debug functions

  def location(name, key) do
    {memtable, indices_table} = get_memtable_and_indices_table(name)
    indices = :ets.tab2list(indices_table)

    case Memtable.read(memtable, key) |> IO.inspect(label: :mt) do
      :error ->
        disktable_location(indices, key)

      {:ok, value} ->
        {:memtable, memtable, value}
    end
  end

  defp disktable_location([], _key), do: :error

  defp disktable_location([{generation, bloom_filter, index, filename} | rest], key) do
    {:ok, fd} = :prim_file.open(filename, [:read])

    bloom_check = BloomFilter.check(bloom_filter, key)
    try do
      Disktable.find_value(fd, index, key)
    else
      {:ok, value} -> {:disktable, generation, fd, bloom_check, value}
      {:error, _} -> disktable_location(rest, key)
    after
      :prim_file.close(fd)
    end
  end

  def debug(name) do
    {_, indices_table} = :persistent_term.get({__MODULE__, name})
    indices = :ets.tab2list(indices_table)
    state = :sys.get_state(name)
    Map.put(state, :indices, indices)
  end

  ## GenServer

  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    data_dir = Keyword.fetch!(opts, :data_dir)
    wal_filename = Path.join(data_dir, "wal")

    memtable = Memtable.new()

    indices_table = :ets.new(:microlsm_indices, [:public, :ordered_set, read_concurrency: true, write_concurrency: false])
    :persistent_term.put({__MODULE__, name}, {memtable, indices_table})

    {:ok, wal_fd} = :prim_file.open(wal_filename, [:read, :append])

    wal_fd
    |> Wal.stream()
    |> Enum.each(fn {_length, batch} ->
      Memtable.write(memtable, batch)
    end)

    data_dir
    |> Path.join("disktables")
    |> tap(&File.mkdir_p/1)
    |> File.ls!()
    |> Enum.map(&String.to_integer/1)
    |> Enum.sort(:asc)
    |> Enum.each(fn generation ->
      filename = Path.join([data_dir, "disktables", Integer.to_string(generation)])
      {:ok, fd} = :prim_file.open(filename, [:read])
      {length, index} = Disktable.load(fd)
      bloom_filter = build_bloom_filter(fd, index, length)
      entry = {generation, bloom_filter, index, filename}
      :ets.insert(indices_table, entry)
    end)

    state = %{
      name: name,
      data_dir: data_dir,
      wal_fd: wal_fd,
      memtable: memtable,
      threshold: Keyword.get(opts, :threshold, 1024),
      block_count: Keyword.get(opts, :block_count, 10),
      max_batch_length: Keyword.get(opts, :max_batch_length, 10 * 1024),
      batch_length: 0,
      batch: [],
      reply_tos: []
    }

    {:ok, state}
  end

  def handle_call({:batch, in_batch}, from, state) do
    %{
      batch_length: batch_length,
      max_batch_length: max_batch_length,
      batch: batch,
      reply_tos: reply_tos
    } = state

    batch_length = batch_length + length(in_batch)

    state = %{
      state
      | batch_length: batch_length,
        batch: :lists.reverse(in_batch) ++ batch,
        reply_tos: [from | reply_tos]
    }

    if batch_length >= max_batch_length do
      state = run_batch(state)
      {:noreply, state}
    else
      {:noreply, state, 0}
    end
  end

  def handle_call({:write, key, value}, from, state) do
    %{reply_tos: reply_tos} = state
    state = %{state | reply_tos: [from | reply_tos]}
    do_handle_write(key, value, state)
  end

  def handle_cast({:async_write, key, value}, state) do
    do_handle_write(key, value, state)
  end

  @compile {:inline, do_handle_write: 3}
  defp do_handle_write(key, value, state) do
    %{
      batch_length: batch_length,
      max_batch_length: max_batch_length,
      batch: batch
    } = state

    batch_length = batch_length + 1

    state = %{
      state
      | batch_length: batch_length,
        batch: [{key, value} | batch]
    }

    if batch_length >= max_batch_length do
      state = run_batch(state)
      {:noreply, state}
    else
      {:noreply, state, 0}
    end
  end

  def handle_info(:timeout, state) do
    state = run_batch(state)
    {:noreply, state}
  end

  ## Helpers

  defp run_batch(state) do
    %{
      memtable: memtable,
      wal_fd: wal_fd,
      batch: batch,
      reply_tos: reply_tos,
      threshold: threshold
    } = state

    batch = :lists.reverse(batch)

    Wal.push_batch(wal_fd, batch)
    Memtable.write(memtable, batch)
    garbage_collect()

    state =
      if Memtable.memory(memtable) > threshold do
        :eflambe.apply({__MODULE__, :dump, [state]}, output_directory: ~c"flamegraphs")
        # dump(state)
      else
        state
      end

    for reply_to <- reply_tos, do: GenServer.reply(reply_to, :ok)

    %{state | batch: [], reply_tos: [], batch_length: 0}
  end

  def dump(state) do
    memtable_read_ahead = 1000

    %{
      name: name,
      memtable: memtable,
      wal_fd: wal_fd,
      data_dir: data_dir,
      block_count: block_count
    } = state

    spawn_link(fn ->
      {^memtable, indices_table} = :persistent_term.get({__MODULE__, name})
      indices = :ets.tab2list(indices_table)

      vencoded_memtable_stream =
        memtable
        |> Memtable.stream(memtable_read_ahead)
        |> Stream.map(fn {key, value} ->
          {key, term_to_binary(value)}
        end)

      {new_indices, to_delete} =
        merge(
          indices,
          Memtable.size(memtable),
          0,
          [],
          vencoded_memtable_stream,
          block_count,
          Path.join(data_dir, "disktables")
        )

      # TODO these lines need optimization
      generations_to_delete = Enum.map(indices, &element(1, &1)) -- Enum.map(new_indices, &element(1, &1))
      :ets.insert(indices_table, new_indices)
      for generation <- generations_to_delete do
        :ets.delete(indices_table, generation)
      end

      for {fd, filename} <- to_delete do
        :prim_file.close(fd)
        :prim_file.delete(filename)
      end

      Memtable.clear(memtable)
      :prim_file.truncate(wal_fd)
    end)

    state
  end

  defp merge(
         [{generation, _bloom_filter, _index, filename} | indices],
         length,
         generation,
         to_delete,
         stream,
         block_count,
         disktables_dir
       ) do
    {:ok, fd} = :prim_file.open(filename, [:read])
    rstream = Disktable.stream(fd)
    rlength = Disktable.total_length(fd)
    stream = Microlsm.Stream.merge_streams(stream, rstream)

    to_delete = [{fd, Path.join(disktables_dir, Integer.to_string(generation))} | to_delete]

    merge(
      indices,
      length + rlength,
      generation + 1,
      to_delete,
      stream,
      block_count,
      disktables_dir
    )
  end

  @encoded_delete term_to_binary({})

  defp merge(indices, length, generation, to_delete, stream, block_count, disktables_dir) do
    filename = Path.join(disktables_dir, Integer.to_string(generation))
    {:ok, fd} = :prim_file.open(filename, [:write, :read])

    {index, bloom_filter} =
      case indices do
        [] ->
          # We just drop tombstones in the oldest generation
          stream
          |> Stream.reject(&match?({_, @encoded_delete}, &1))
          |> Disktable.write_stream(fd, length, block_count)

        _ ->
          Disktable.write_stream(stream, fd, length, block_count)
      end

    :prim_file.close(fd)
    {[{generation, bloom_filter, index, filename} | indices], to_delete}
  end

  defp build_bloom_filter(fd, index, length) do
    bitsize = 10 * length
    k = 6
    filter = BloomFilter.new(bitsize, k)

    fd
    |> Disktable.stream(index)
    |> Enum.reduce(filter, fn {key, _value}, filter -> BloomFilter.add(filter, key) end)
    |> BloomFilter.finalize()
  end

  defp swap_memtables(name) do
    {atomics_ref, _mt1, _mt2, _indices} = :persistent_term.get({__MODULE__, name})
    old = :atomics.get(atomics_ref, 1)
    new = 1 - old
    :ok = :atomics.compare_exchange(atomics_ref, 1, old, new)
  end

  defp get_memtable_and_indices_table(name) do
    {atomics_ref, mt1, mt2, it1, it2} = :persistent_term.get({__MODULE__, name})
    case :atomics.get(atomics_ref, 1) do
      0 -> {mt1, it1}
      1 -> {mt2, it2}
    end
  end
end
