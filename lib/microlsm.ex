defmodule Microlsm do
  @moduledoc """
  Microlsm is a tiny and simple LSM Key-Value storage
  """

  use GenServer

  alias Microlsm.BloomFilter
  alias Microlsm.Disktable
  alias Microlsm.Memtable
  alias Microlsm.Wal

  import :erlang, only: [term_to_binary: 1, binary_to_term: 1, term_to_iovec: 1]

  require Record
  Record.defrecordp(:disktable,
    generation: nil,
    filename: nil,
    bloom_filter: nil,
    index: nil
  )

  @memtable_read_ahead 1_000

  @call_timeout 5_000

  @batch_timeout 0

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

  debug? = false

  if debug? do
    @debug_queue_size 30
    defmacrop dlog(msg) do
      quote do: do_dlog(unquote(msg))
    end

    defp do_dlog(msg) do
      queue =
        with nil <- Process.get(:__debug_queue) do
          :queue.from_list List.duplicate(nil, @debug_queue_size)
        end

      queue = :queue.drop(queue)
      msg = {:erlang.system_time(:millisecond), msg}
      queue = :queue.in(msg, queue)
      Process.put(:__debug_queue, queue)
    end
  else
    defmacrop dlog(_), do: nil
  end

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
    #   :eflambe.apply({__MODULE__, :do_read, [name, key]}, output_directory: ~c"flamegraphs")
    # end
    # def do_read(name, key) do

    {atomics_ref, mt1, mt2, it1, it2} =
      with nil <- :persistent_term.get({__MODULE__, name}) do
        raise ArgumentError, "No such table #{name}"
      end

    {memtable, shadow_memtable} =
      case :atomics.get(atomics_ref, 1) do
        0 -> {mt1, mt2}
        1 -> {mt2, mt1}
      end

    result =
      with(
        :error <- Memtable.read(memtable, key),
        :error <- Memtable.read(shadow_memtable, key)
      ) do
        indices_table =
          case :atomics.get(atomics_ref, 2) do
            0 -> it1
            1 -> it2
          end

        indices = :ets.match_object(indices_table, :_)
        disktable_read(indices, key)
      end

    case result do
      {:ok, {value}} -> {:ok, value}
      _ -> :error
    end
  end

  defp disktable_read([], _key), do: :error

  defp disktable_read([disktable | rest], key) do
    disktable(bloom_filter: bloom_filter, index: index, filename: filename) = disktable

    case Disktable.check_bounds(index, key) do
      x when x == :in or :erlang.element(1, x) == :exact ->
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

      _ ->
        disktable_read(rest, key)
    end
  end

  @spec all(t()) :: Enumerable.t({key(), value()})
  def all(name) do
    maybe_raise_when_no_table(name)
    {memtable, shadow_memtable, index_fds} = prepare_stream_read(name)
    memtable_stream = Memtable.stream(memtable, @memtable_read_ahead)
    shadow_memtable_stream = Memtable.stream(shadow_memtable, @memtable_read_ahead)

    all_memtables_stream = Microlsm.Stream.merge_streams(memtable_stream, shadow_memtable_stream)

    index_fds
    |> Enum.reduce(all_memtables_stream, fn {index, fd}, stream ->
      disk_stream = Disktable.stream(fd, index)
      Microlsm.Stream.merge_streams(stream, disk_stream)
    end)
    |> finalize_read_stream(index_fds)
  end

  @spec range_read(t(), key(), key()) :: Enumerable.t({key(), value()})
  def range_read(name, left_key, right_key) when left_key <= right_key do
    maybe_raise_when_no_table(name)
    {memtable, shadow_memtable, index_fds} = prepare_stream_read(name)
    memtable_stream = Memtable.range_read(memtable, left_key, right_key, @memtable_read_ahead)
    shadow_memtable_stream = Memtable.range_read(shadow_memtable, left_key, right_key, @memtable_read_ahead)

    all_memtables_stream = Microlsm.Stream.merge_streams(memtable_stream, shadow_memtable_stream)

    index_fds
    |> Enum.reduce(all_memtables_stream, fn {index, fd}, stream ->
      disk_stream = Disktable.range_stream(fd, index, left_key, right_key)
      Microlsm.Stream.merge_streams(stream, disk_stream)
    end)
    |> finalize_read_stream(index_fds)
  end

  defp finalize_read_stream(stream, index_fds) do
    fds = for {_index, fd} <- index_fds, do: fd

    stream
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
    |> Microlsm.Stream.after_hook(fn ->
      Enum.each(fds, &:prim_file.close/1)
    end)
  end

  @spec ranges_read(t(), [{key(), key()}]) :: Enumerable.t({key(), value()})
  def ranges_read(name, [{left_key, right_key} | ranges]) do
    Enum.reduce(ranges, range_read(name, left_key, right_key), fn {left_key, right_key}, stream ->
      range_stream = range_read(name, left_key, right_key)
      Stream.concat(stream, range_stream)
    end)
  end

  defp prepare_stream_read(name) do
    {memtable, shadow_memtable, indices_table} = get_memtable_and_indices_table(name)
    indices = :ets.match_object(indices_table, :_)
    index_fds =
      Enum.map(indices, fn disktable(index: index, filename: filename) ->
        {:ok, fd} = :prim_file.open(filename, [:read])
        {index, fd}
      end)

    {memtable, shadow_memtable, index_fds}
  end

  ## Debug functions

  def location(name, key) do
    {memtable, shadow_memtable, indices_table} = get_memtable_and_indices_table(name)
    indices = :ets.match_object(indices_table, :_)

    case Memtable.read(memtable, key) do
      :error ->
        case Memtable.read(shadow_memtable, key) do
          {:ok, value} ->
            {:shadow_memtable, shadow_memtable, value}

          :error ->
            disktable_location(indices, key)
        end

      {:ok, value} ->
        {:memtable, memtable, value}
    end
  end

  defp disktable_location([], _key), do: :error

  defp disktable_location([disktable | rest], key) do
    disktable(generation: generation, bloom_filter: bloom_filter, index: index, filename: filename) = disktable
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
    {_, _, indices_table} = get_memtable_and_indices_table(name)
    indices = :ets.match_object(indices_table, :_)
    state = :sys.get_state(name)
    state = Map.put(state, :indices, indices)
    unquote(
      if debug? do
        quote do
          {_, dict} = Process.info(Process.whereis(var!(name)), :dictionary)
          q = :queue.to_list Keyword.get(dict, :__debug_queue, {[], []})
          Map.put(var!(state), :dlog, q)
        end
      else
        quote do: var!(state)
      end
    )
  end

  ## GenServer

  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    data_dir = Path.expand(Keyword.fetch!(opts, :data_dir))
    check_or_write_lock(data_dir)
    disktable_dir = Path.join(data_dir, "disktables")

    wal_dir = Path.join(data_dir, "wal")
    File.mkdir_p!(wal_dir)

    [{memtable, indices_table}, {shadow_memtable, shadow_indices_table}] =
      for _ <- 1..2 do
        memtable = Memtable.new()
        indices_table = :ets.new(:microlsm_indices, [:public, :ordered_set, read_concurrency: true, write_concurrency: false, keypos: 2])
        {memtable, indices_table}
      end

    {wal_fd, shadow_wal_fd, wal_swap_flag_fd} = restore_wals(memtable, shadow_memtable, wal_dir)

    data_dir
    |> Path.join("disktables")
    |> tap(&File.mkdir_p/1)
    |> File.ls!()
    |> Enum.map(&String.to_integer/1)
    |> Enum.sort(:asc)
    |> Enum.each(fn generation ->
      filename = Path.join(disktable_dir, Integer.to_string(generation))
      {:ok, fd} = :prim_file.open(filename, [:read])
      {length, index} = Disktable.load(fd)
      bloom_filter = build_bloom_filter(fd, index, length)
      entry = disktable(generation: generation, bloom_filter: bloom_filter, index: index, filename: filename)
      :ets.insert(indices_table, entry)
    end)

    state = %{
      merge_ref: nil,
      name: name,
      data_dir: data_dir,

      wal_fd: wal_fd,
      shadow_wal_fd: shadow_wal_fd,
      wal_swap_flag_fd: wal_swap_flag_fd,

      threshold: div(Keyword.get(opts, :threshold, 1024), 2),
      block_count: Keyword.get(opts, :block_count, 10),
      max_batch_length: Keyword.get(opts, :max_batch_length, 10 * 1024),
      batch_length: 0,
      batch: [],
      reply_tos: []
    }

    atomics_ref = :atomics.new(2, signed: false)
    :persistent_term.put({__MODULE__, name}, {atomics_ref, memtable, shadow_memtable, indices_table, shadow_indices_table})
    {:ok, state, {:continue, :dump}}
  end

  def handle_continue(:dump, state) do
    dlog(:continue_dump)
    %{batch_length: batch_length, max_batch_length: max_batch_length} = state

    if batch_length >= max_batch_length do
      state = run_batch(state)
      dlog(:not_setting_dump_timeout)
      {:noreply, state}
    else
      dlog(:setting_dump_timeout)
      {:noreply, state, @batch_timeout}
    end
  end

  defp restore_wals(memtable, shadow_memtable, wal_dir) do
    full_current_path = Path.expand(Path.join(wal_dir, "current"))
    {:ok, wal_swap_flag_fd} = :prim_file.open(full_current_path, [:read, :write])

    case :prim_file.pread(wal_swap_flag_fd, 0, 1) do
      :eof ->
        :prim_file.pwrite(wal_swap_flag_fd, 0, "0")
        :prim_file.sync(wal_swap_flag_fd)

        {restore_wal(wal_dir, "0", memtable), restore_wal(wal_dir, "1", shadow_memtable), wal_swap_flag_fd}

      {:ok, "0"} ->
        {restore_wal(wal_dir, "0", memtable), restore_wal(wal_dir, "1", shadow_memtable), wal_swap_flag_fd}

      {:ok, "1"} ->
        {restore_wal(wal_dir, "1", memtable), restore_wal(wal_dir, "0", shadow_memtable), wal_swap_flag_fd}
    end
  end

  defp restore_wal(wal_dir, wal_filename, memtable) do
    full_wal_path = Path.expand(Path.join(wal_dir, wal_filename))
    {:ok, wal_fd} = :prim_file.open(full_wal_path, [:read, :append])

    wal_fd
    |> Wal.stream()
    |> Enum.each(fn {_length, batch} ->
      Memtable.write(memtable, batch)
    end)

    wal_fd
  end

  def handle_call({:batch, in_batch}, from, state) do
    dlog(:continue_batch)
    %{batch_length: batch_length, batch: batch, reply_tos: reply_tos} = state

    batch_length = batch_length + length(in_batch)

    state = %{
      state
      | batch_length: batch_length,
        batch: :lists.reverse(in_batch) ++ batch,
        reply_tos: [from | reply_tos]
    }

    {:noreply, state, {:continue, :dump}}
  end

  def handle_call({:write, key, value}, from, state) do
    dlog(:continue_write)
    %{reply_tos: reply_tos} = state
    state = %{state | reply_tos: [from | reply_tos]}
    do_handle_write(key, value, state)
  end

  def handle_cast({:async_write, key, value}, state) do
    dlog(:continue_async_write)
    do_handle_write(key, value, state)
  end

  @compile {:inline, do_handle_write: 3}
  defp do_handle_write(key, value, state) do
    %{batch_length: batch_length, batch: batch} = state

    batch_length = batch_length + 1

    state = %{
      state
      | batch_length: batch_length,
        batch: [{key, value} | batch]
    }

    {:noreply, state, {:continue, :dump}}
  end

  def handle_info({:microlsm_merge_done, ref}, %{merge_ref: ref} = state) do
    state = finish_dump(state)
    {:noreply, state, {:continue, :dump}}
  end

  def handle_info(:timeout, state) do
    dlog(:received_dump_timeout)
    state = run_batch(state)
    {:noreply, state}
  end

  ## Helpers

  defp run_batch(%{batch_length: 0} = state) do
    state
  end

  defp run_batch(state) do
    %{
      name: name,
      wal_fd: wal_fd,
      batch: batch,
      reply_tos: reply_tos,
      threshold: threshold,
      merge_ref: merge_ref
    } = state
    {memtable, _, _} = get_memtable_and_indices_table(name)

    batch = :lists.reverse(batch)
    Wal.push_batch(wal_fd, batch)
    Memtable.write(memtable, batch)

    state =
      if is_nil(merge_ref) and (Memtable.memory(memtable) > threshold) do
        # :eflambe.apply({__MODULE__, :dump, [state]}, output_directory: ~c"flamegraphs")
        start_dump(state)
      else
        state
      end

    mreply(reply_tos)

    %{state | batch: [], reply_tos: [], batch_length: 0}
  end

  defp mreply([reply_to | reply_tos]) do
    :gen.reply(reply_to, :ok)
    mreply(reply_tos)
  end

  defp mreply([]) do
    []
  end

  ## Compaction

  defp start_dump(state) do
    %{block_count: block_count, data_dir: data_dir, name: name} = state

    {memtable, _shadow_memtable, indices_table} = get_memtable_and_indices_table(name)
    merge_ref = spawn_dumper(indices_table, memtable, block_count, data_dir, name)

    swap_memtables(name)
    state = swap_wals(state)

    %{state | merge_ref: merge_ref}
  end

  defp spawn_dumper(indices_table, memtable, block_count, data_dir, name) do
    owner = self()
    merge_ref = make_ref()

    spawn_link(fn ->
      indices = :ets.match_object(indices_table, :_)

      vencoded_memtable_stream =
        memtable
        |> Memtable.stream(@memtable_read_ahead)
        |> Stream.map(fn {key, value} ->
          {key, term_to_iovec(value)}
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

      Memtable.clear(memtable)
      shadow_indices_table = get_shadow_indices_table(name)

      :ets.delete_all_objects(shadow_indices_table)
      :ets.insert(shadow_indices_table, new_indices)

      swap_index_tables(name)

      for {fd, filename} <- to_delete do
        :prim_file.close(fd)
        :prim_file.delete(filename)
      end

      send(owner, {:microlsm_merge_done, merge_ref})
    end)

    merge_ref
  end

  defp finish_dump(state) do
    %{shadow_wal_fd: wal_fd} = state
    Wal.truncate(wal_fd)
    %{state | merge_ref: nil}
  end

  defp merge(
         [disktable(generation: generation, filename: filename) | indices],
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

  @encoded_deletes [term_to_binary({}), term_to_iovec({})]

  defp merge(indices, length, generation, to_delete, stream, block_count, disktables_dir) do
    filename = Path.join(disktables_dir, Integer.to_string(generation))
    {:ok, fd} = :prim_file.open(filename, [:write, :read])

    {index, bloom_filter} =
      case indices do
        [] ->
          # We just drop tombstones in the oldest generation
          stream
          |> Stream.reject(&match?({_, value} when value in @encoded_deletes, &1))
          |> Disktable.write_stream(fd, length, block_count)

        _ ->
          Disktable.write_stream(stream, fd, length, block_count)
      end

    :prim_file.close(fd)
    disktable = disktable(generation: generation, bloom_filter: bloom_filter, index: index, filename: filename)
    {[disktable | indices], to_delete}
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

  ## Safety checks

  defp maybe_raise_when_no_table(name) do
    with nil <- Process.whereis(name) do
      raise ArgumentError, "No such table #{name}"
    end
  end

  defp check_or_write_lock(data_dir) do
    full_lock_path = Path.join(data_dir, "lock")
    {:ok, fd} = :prim_file.open(full_lock_path, [:read, :write])

    try do
      with(
        {:ok, data} <- :prim_file.pread(fd, 0, 4 * 1024),
        %{os: os, pid: pid} <- binary_to_term(data),
        ^os <- System.pid(),
        true <- Process.alive?(pid)
      ) do
        raise "Table is in use by #{inspect(pid)}"
      else
        _ -> write_lock(fd)
      end
    after
      :prim_file.close(fd)
    end
  end

  defp write_lock(fd) do
    data = term_to_iovec(%{os: System.pid(), pid: self()})
    :prim_file.pwrite(fd, 0, data)
  end

  ## Compation swaping

  defp swap_wals(state) do
    %{wal_fd: working_wal, shadow_wal_fd: shadow_wal, wal_swap_flag_fd: wal_swap_flag_fd} = state

    to_write =
      case :prim_file.pread(wal_swap_flag_fd, 0, 8) do
        {:ok, "0"} -> "1"
        {:ok, "1"} -> "0"
      end

    :prim_file.pwrite(wal_swap_flag_fd, 0, to_write)
    :prim_file.sync(wal_swap_flag_fd)

    %{state | wal_fd: shadow_wal, shadow_wal_fd: working_wal}
  end

  defp swap_memtables(name) do
    do_swap_tables(name, 1)
  end

  defp swap_index_tables(name) do
    do_swap_tables(name, 2)
  end

  defp do_swap_tables(name, index) do
    {atomics_ref, _mt1, _mt2, _it1, _it2} = :persistent_term.get({__MODULE__, name})
    old = :atomics.get(atomics_ref, index)
    new = 1 - old
    :ok = :atomics.compare_exchange(atomics_ref, index, old, new)
  end

  defp get_memtable_and_indices_table(name) do
    {atomics_ref, mt1, mt2, it1, it2} = :persistent_term.get({__MODULE__, name})

    case {:atomics.get(atomics_ref, 1), :atomics.get(atomics_ref, 2)} do
      {0, 0} -> {mt1, mt2, it1}
      {1, 0} -> {mt2, mt1, it1}
      {0, 1} -> {mt1, mt2, it2}
      {1, 1} -> {mt2, mt1, it2}
    end
  end

  defp get_shadow_indices_table(name) do
    {atomics_ref, _mt1, _mt2, it1, it2} = :persistent_term.get({__MODULE__, name})

    case :atomics.get(atomics_ref, 2) do
      1 -> it1
      0 -> it2
    end
  end
end
