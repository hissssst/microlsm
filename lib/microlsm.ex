defmodule Microlsm do
  @moduledoc """
  Microlsm is a tiny and simple LSM Key-Value storage
  """

  use GenServer

  alias Microlsm.AtomicFlags
  alias Microlsm.BloomFilter
  alias Microlsm.DescriptorPool
  alias Microlsm.Disktable
  alias Microlsm.Gentable
  alias Microlsm.Memtable
  alias Microlsm.Wal

  import :erlang, only: [term_to_binary: 1, binary_to_term: 1, term_to_iovec: 1]
  import Gentable, only: [disktable: 1]
  import Record, only: [defrecordp: 2]

  defrecordp :global_state,
    # State
    atomics_ref: nil,
    memtable1: nil,
    memtable2: nil,
    gentable1: nil,
    gentable2: nil,
    descriptor_pool: nil,
    # Config
    block_size_threshold: nil,
    memtable_read_ahead: nil

  require ODCounter

  @call_timeout 5_000

  @type key :: any()

  @type value :: any()

  @type t :: atom()

  @type start_option ::
          {:name, t()}
          | {:data_dir, Path.t()}
          | {:descriptor_pool_size, pos_integer()}
          | {:threshold, bytes :: pos_integer()}
          | {:block_size_threshold, bytes :: pos_integer()}
          | {:max_batch_length, pos_integer()}
          | {:memtable_read_ahead, pos_integer()}

  @type batch_operation ::
          {:delete, key()}
          | {:write, key(), value()}

  @type call_timeout_option :: {:timeout, timeout()}

  import Microlsm.Debug

  ## Public API

  @spec start_link([start_option()]) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(Microlsm, opts, name: name)
  end

  @spec clean(t(), [call_timeout_option()]) :: :ok
  def clean(name, opts \\ []) do
    GenServer.call(name, :clean, call_timeout(opts))
  end

  @spec write(t(), key(), value(), [call_timeout_option()]) :: :ok
  def write(name, key, value, opts \\ []) do
    GenServer.call(name, {:write, key, {value}}, call_timeout(opts))
  end

  @spec write_nosync(t(), key(), value(), [call_timeout_option()]) :: :ok
  def write_nosync(name, key, value, opts \\ []) do
    GenServer.call(name, {:write_nosync, key, {value}}, call_timeout(opts))
  end

  @spec async_write(t(), key(), value()) :: :ok
  def async_write(name, key, value) do
    GenServer.cast(name, {:async_write, key, {value}})
    :ok
  end

  @spec delete(t(), key(), [call_timeout_option()]) :: :ok
  def delete(name, key, opts \\ []) do
    GenServer.call(name, {:write, key, {}}, call_timeout(opts))
  end

  @spec async_delete(t(), key()) :: :ok
  def async_delete(name, key) do
    GenServer.cast(name, {:async_write, key, {}})
    :ok
  end

  @spec batch(t(), [batch_operation()], [call_timeout_option()]) :: :ok
  def batch(name, batch, opts \\ []) do
    batch =
      Enum.map(batch, fn
        {:write, key, value} -> {key, {value}}
        {:delete, key} -> {key, {}}
      end)

    GenServer.call(name, {:batch, batch}, call_timeout(opts))
  end

  @compile {:inline, call_timeout: 1}
  defp call_timeout(opts) do
    Keyword.get(opts, :timeout, @call_timeout)
  end

  @spec read(t(), key()) :: {:ok, value()} | :error
  def read(name, key) do
    #   :eflambe.apply({Microlsm, :do_read, [name, key]}, output_directory: ~c"flamegraphs")
    # end
    # def do_read(name, key) do
    maybe_raise_when_no_table(name)

    global_state(
      atomics_ref: atomics_ref,
      block_size_threshold: block_size_threshold,
      memtable1: mt1,
      memtable2: mt2,
      gentable1: gt1,
      gentable2: gt2,
      descriptor_pool: descriptor_pool
    ) =
      with nil <- :persistent_term.get({Microlsm, name}) do
        raise ArgumentError, "No such table #{name}"
      end

    case AtomicFlags.select(atomics_ref, :reading, :enabled, :disabled) do
      :enabled ->
        {memtable, shadow_memtable} = AtomicFlags.order(atomics_ref, :memtables, mt1, mt2)

        result =
          with(
            :error <- Memtable.read(memtable, key),
            :error <- Memtable.read(shadow_memtable, key)
          ) do
            ODCounter.add(Microlsm, name, :memtable_miss)
            gentable = AtomicFlags.select(atomics_ref, :gentables, gt1, gt2)
            generations = Gentable.to_list(gentable)
            disktable_read(generations, key, descriptor_pool, name, block_size_threshold)
          else
            result ->
              ODCounter.add(Microlsm, name, :memtable_hit)
              result
          end

        case result do
          {:ok, {value}} -> {:ok, value}
          _ -> :error
        end

      :disabled ->
        :error
    end
  end

  defp disktable_read([], _key, _descriptor_pool, name, _block_size) do
    ODCounter.add(Microlsm, name, :complete_miss)
    :error
  end

  defp disktable_read([{_generation, disktables} | rest], key, descriptor_pool, name, block_size) do
    case Gentable.search_disktable_in_generation(disktables, key) do
      disktable(
        bloom_filter: bloom_filter,
        index: index,
        descriptor_pool_state: descriptor_pool_state
      ) ->
        ODCounter.add(Microlsm, name, :generation_hit)
        case BloomFilter.check(bloom_filter, key) do
          :no ->
            ODCounter.add(Microlsm, name, :bloom_filter_miss)
            disktable_read(rest, key, descriptor_pool, name, block_size)

          :maybe ->
            ODCounter.add(Microlsm, name, :bloom_filter_hit)
            result = Disktable.find_block(index, key)

            DescriptorPool.checkout(descriptor_pool, descriptor_pool_state, fn fd ->
              Disktable.find_value(fd, result, key, block_size)
            end)
            |> case do
              {:ok, value} ->
                ODCounter.add(Microlsm, name, :disktable_hit)
                {:ok, value}

              {:error, _} ->
                ODCounter.add(Microlsm, name, :disktable_miss)
                disktable_read(rest, key, descriptor_pool, name, block_size)
            end
        end

      :not_found ->
        ODCounter.add(Microlsm, name, :generation_miss)
        disktable_read(rest, key, descriptor_pool, name, block_size)
    end
  end


  @spec all(t()) :: Enumerable.t({key(), value()})
  def all(name) do
    maybe_raise_when_no_table(name)
    {memtable, shadow_memtable, gentable, _descriptor_pool, memtable_read_ahead} = get_memtable_and_gentable(name)
    generations = Gentable.to_list(gentable)

    memtable_stream = Memtable.stream(memtable, memtable_read_ahead)
    shadow_memtable_stream = Memtable.stream(shadow_memtable, memtable_read_ahead)

    all_memtables_stream = Microlsm.Stream.merge_streams(memtable_stream, shadow_memtable_stream)

    # Otherwise we would get funny inconsistencies
    all_memtables_list = Enum.to_list(all_memtables_stream)

    {fds, stream} =
      Enum.map_reduce(generations, all_memtables_list, fn disktable(filename: filename, index: index), stream ->
        {:ok, fd} = :prim_file.open(filename, [:read])
        disk_stream = Disktable.stream(fd, index)
        stream = Microlsm.Stream.merge_streams(stream, disk_stream)
        {fd, stream}
      end)

    finalize_read_stream(stream, fds)
  end

  @spec range_read(t(), key(), key()) :: Enumerable.t({key(), value()})
  def range_read(name, left_key, right_key) when left_key <= right_key do
    maybe_raise_when_no_table(name)
    {memtable, shadow_memtable, gentable, _descriptor_pool, memtable_read_ahead} = get_memtable_and_gentable(name)
    generations = Gentable.to_list(gentable)
    memtable_stream = Memtable.range_read(memtable, left_key, right_key, memtable_read_ahead)
    shadow_memtable_stream = Memtable.range_read(shadow_memtable, left_key, right_key, memtable_read_ahead)

    all_memtables_stream = Microlsm.Stream.merge_streams(memtable_stream, shadow_memtable_stream)

    # Otherwise we would get funny inconsistencies
    all_memtables_list = Enum.to_list(all_memtables_stream)

    {fds, stream} =
      Enum.flat_map_reduce(generations, all_memtables_list, fn disktable(filename: filename, index: index), stream ->
        case Disktable.range_offsets(index, left_key, right_key) do
          [] ->
            {[], stream}

          offsets ->
            {:ok, fd} = :prim_file.open(filename, [:read])
            disk_stream = Disktable.range_stream(fd, left_key, right_key, offsets)
            stream = Microlsm.Stream.merge_streams(stream, disk_stream)
            {[fd], stream}
        end
      end)

    finalize_read_stream(stream, fds)
  end

  defp finalize_read_stream(stream, fds) do
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

  ## Debug functions

  def location(name, key) do
    {memtable, shadow_memtable, gentable, _descriptor_pool, _memtable_read_ahead} = get_memtable_and_gentable(name)
    generations = Gentable.to_list(gentable)

    case Memtable.read(memtable, key) do
      :error ->
        case Memtable.read(shadow_memtable, key) do
          {:ok, value} ->
            {:shadow_memtable, shadow_memtable, value}

          :error ->
            disktable_location(generations, key)
        end

      {:ok, value} ->
        {:memtable, memtable, value}
    end
  end

  defp disktable_location([], _key), do: :error

  defp disktable_location([{_generation, disktables} | rest], key) do
    disktable = Gentable.search_disktable_in_generation(disktables, key)

    disktable(generation: generation, bloom_filter: bloom_filter, index: index, filename: filename) = disktable
    {:ok, fd} = :prim_file.open(filename, [:read])

    bloom_check = BloomFilter.check(bloom_filter, key)
    try do
      block = Disktable.find_block(index, key)
      Disktable.find_value(fd, block, key)
    else
      {:ok, value} -> {:disktable, generation, bloom_check, value, filename}
      {:error, _} -> disktable_location(rest, key)
    after
      :prim_file.close(fd)
    end
  end

  def debug(name) do
    pid = Process.whereis(name)
    {_, _, gentable, _descriptor_pool, _memtable_read_ahead} = get_memtable_and_gentable(name)
    generations = Gentable.to_list(gentable)
    state = :sys.get_state(pid)
    state = Map.put(state, :generations, generations)
    Map.put(state, :dlog, get_dlog(pid))
  end

  ## GenServer

  def init(opts) do
    name = Keyword.fetch!(opts, :name)

    ODCounter.init_schema(Microlsm, runtime_size: 10, ignore_if_exists: true)
    ODCounter.new(Microlsm, name, ignore_if_exists: true)
    ODCounter.reset(Microlsm, name)

    data_dir = Path.expand(Keyword.fetch!(opts, :data_dir))
    check_or_write_lock(data_dir)
    disktable_dir = Path.join(data_dir, "disktables")

    block_size_threshold = Keyword.get(opts, :block_size_threshold, 4 * 1024)
    memtable_read_ahead = Keyword.get(opts, :memtable_read_ahead, 1000)
    batch_timeout = Keyword.get(opts, :batch_timeout, 0)
    threshold = Keyword.get(opts, :threshold, 1024)
    max_batch_length = Keyword.get(opts, :max_batch_length, 10 * 1024)
    descriptor_pool_size = Keyword.get(opts, :descriptor_pool_size, 1)

    wal_dir = Path.join(data_dir, "wal")
    File.mkdir_p!(wal_dir)

    memtable = Memtable.new()
    shadow_memtable = Memtable.new()
    gentable = Gentable.new()
    shadow_gentable = Gentable.new()

    {wal_fd, shadow_wal_fd, wal_swap_flag_fd} = restore_wals(memtable, shadow_memtable, wal_dir)
    descriptor_pool = DescriptorPool.new()

    with :ok <- File.mkdir_p(disktable_dir) do
      dlog(:mkdir_p_disktable_dir)
    end

    restore_disktables(disktable_dir, descriptor_pool, gentable, descriptor_pool_size)

    state = %{
      merge_ref: nil,
      name: name,
      data_dir: data_dir,

      block_size_threshold: block_size_threshold,
      batch_timeout: batch_timeout,
      threshold: threshold,
      max_batch_length: max_batch_length,
      descriptor_pool_size: descriptor_pool_size,

      wal_fd: wal_fd,
      shadow_wal_fd: shadow_wal_fd,
      wal_swap_flag_fd: wal_swap_flag_fd,

      batch_length: 0,
      batch: [],
      reply_tos: []
    }

    atomics_ref = AtomicFlags.new()
    global_state =
      global_state(
        atomics_ref: atomics_ref,
        block_size_threshold: block_size_threshold,
        memtable1: memtable,
        memtable2: shadow_memtable,
        gentable1: gentable,
        gentable2: shadow_gentable,
        descriptor_pool: descriptor_pool,
        memtable_read_ahead: memtable_read_ahead
      )

    :persistent_term.put({Microlsm, name}, global_state)
    {:ok, state, {:continue, :init_dump}}
  end

  defp restore_disktables(disktable_dir, descriptor_pool, gentable, descriptor_pool_size) do
    generations =
      disktable_dir
      |> File.ls!()
      |> Enum.map(fn filename ->
        [generation, id] = String.split(filename, "_")
        {String.to_integer(generation), id, Path.join(disktable_dir, filename)}
      end)
      |> Enum.reduce(%{}, fn {generation, id, filename}, groups ->
        {:ok, fd} = :prim_file.open(filename, [:read])

        case Disktable.load(fd) do
          :broken ->
            :ok = :prim_file.close(fd)
            :prim_file.delete(filename)
            groups

          {length, index} ->
            bloom_filter = build_bloom_filter(fd, index, length)
            descriptor_pool_state = DescriptorPool.add(descriptor_pool, filename, descriptor_pool_size)

            entry =
              disktable(
                id: id,
                generation: generation,
                bloom_filter: bloom_filter,
                index: index,
                filename: filename,
                descriptor_pool_state: descriptor_pool_state
              )

            case groups do
              %{^generation => entries} -> %{groups | generation => [entry | entries]}
              %{} -> Map.put(groups, generation, [entry])
            end
        end
      end)

    Gentable.insert(gentable, generations)
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
    {:ok, wal_fd} = :prim_file.open(full_wal_path, [:read, :append, :write])

    wal_fd
    |> Wal.stream()
    |> Enum.each(fn {_length, batch} ->
      Memtable.write(memtable, batch)
    end)

    wal_fd
  end

  def handle_continue(:dump, state) do
    dlog(:continue_dump)
    %{batch_length: batch_length, max_batch_length: max_batch_length, batch_timeout: batch_timeout} = state

    if batch_length >= max_batch_length do
      state = run_batch(state)
      dlog(:not_setting_dump_timeout)
      {:noreply, state}
    else
      dlog(:setting_dump_timeout)
      {:noreply, state, batch_timeout}
    end
  end

  def handle_continue(:init_dump, state) do
    dlog(:continue_init_dump)
    %{data_dir: data_dir, name: name, block_size_threshold: block_size_threshold, descriptor_pool_size: descriptor_pool_size} = state

    {_memtable, shadow_memtable, gentable, descriptor_pool, memtable_read_ahead} = get_memtable_and_gentable(name)

    if Memtable.size(shadow_memtable) > 0 do
      merge_ref = spawn_merge(gentable, shadow_memtable, data_dir, name, descriptor_pool, block_size_threshold, descriptor_pool_size, memtable_read_ahead)
      {:noreply, %{state | merge_ref: merge_ref}}
    else
      {:noreply, state}
    end
  end

  def handle_call(:clean, _from, state) do
    state = maybe_await_merge_finish(state)
    %{name: name, wal_fd: working_wal, shadow_wal_fd: shadow_wal} = state
    global_state(
      atomics_ref: atomics_ref,
      memtable1: mt1,
      memtable2: mt2,
      gentable1: gt1,
      gentable2: gt2,
      descriptor_pool: descriptor_pool
    ) = :persistent_term.get({Microlsm, name})

    :enabled = AtomicFlags.select(atomics_ref, :reading, :enabled, :disabled)
    AtomicFlags.swap(atomics_ref, :reading)

    Memtable.clear(mt1)
    Memtable.clear(mt2)

    DescriptorPool.clean(descriptor_pool)

    Gentable.remove(gt1)
    Gentable.remove(gt2)

    Wal.truncate(working_wal)
    Wal.truncate(shadow_wal)

    :disabled = AtomicFlags.select(atomics_ref, :reading, :enabled, :disabled)
    AtomicFlags.swap(atomics_ref, :reading)

    {:reply, :ok, state}
  end

  def handle_call({:batch, []}, _from, state) do
    {:reply, :ok, state}
  end

  def handle_call({:batch, in_batch}, from, state) do
    dlog(:continue_batch)
    %{batch_length: batch_length, batch: batch, reply_tos: reply_tos} = state
    {batch_length, batch} = batch_to_batch(in_batch, batch, batch_length)

    state = %{
      state
      | batch_length: batch_length,
        batch: batch,
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

  def handle_call({:write_nosync, key, value}, from, state) do
    dlog(:continue_write_nosync)
    %{name: name} = state
    {memtable, _, _, _, _memtable_read_ahead} = get_memtable_and_gentable(name)
    Memtable.write(memtable, key, value)
    GenServer.reply(from, :ok)
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
    state = finish_merge(state)
    {:noreply, state, {:continue, :dump}}
  end

  def handle_info(:timeout, state) do
    dlog(:received_dump_timeout)
    state = run_batch(state)
    {:noreply, state}
  end

  ## Helpers

  defp batch_to_batch([entry | in_batch], out_batch, len) do
    batch_to_batch(in_batch, [entry | out_batch], len + 1)
  end

  defp batch_to_batch([], out_batch, len) do
    {len, out_batch}
  end

  defp run_batch(%{batch_length: 0} = state) do
    state
  end

  defp run_batch(state) do
    %{
      name: name,
      wal_fd: wal_fd,
      batch: batch,
      reply_tos: reply_tos,
      threshold: threshold
    } = state
    {memtable, _, _, _, _} = get_memtable_and_gentable(name)

    batch = :lists.reverse(batch)
    Wal.push_batch(wal_fd, batch)
    Memtable.write(memtable, batch)

    state =
      if Memtable.memory(memtable) > threshold do
        state
        |> maybe_await_merge_finish()
        |> start_dump()
      else
        state
      end

    mreply(reply_tos)

    %{state | batch: [], reply_tos: [], batch_length: 0}
  end

  defp maybe_await_merge_finish(%{merge_ref: nil} = state) do
    state
  end

  defp maybe_await_merge_finish(%{merge_ref: merge_ref, name: name} = state) do
    ODCounter.add(Microlsm, name, :dumps_late)
    receive do
      {:microlsm_merge_done, ^merge_ref} ->
        finish_merge(state)
    end
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
    %{data_dir: data_dir, name: name, block_size_threshold: block_size_threshold, descriptor_pool_size: descriptor_pool_size} = state
    ODCounter.add(Microlsm, name, :dumps_started)

    {memtable, _shadow_memtable, gentable, descriptor_pool, memtable_read_ahead} = get_memtable_and_gentable(name)

    merge_ref = spawn_merge(gentable, memtable, data_dir, name, descriptor_pool, block_size_threshold, descriptor_pool_size, memtable_read_ahead)

    state = swap_wals(state)
    swap_memtables(name)

    %{state | merge_ref: merge_ref}
  end

  defp spawn_merge(gentable, memtable, data_dir, name, descriptor_pool, block_size_threshold, descriptor_pool_size, memtable_read_ahead) do
    owner = self()
    merge_ref = make_ref()

    spawn_link(fn ->
      Process.put(:microlsm_name, name)
      generations = Gentable.to_list(gentable)

      vencoded_memtable_stream =
        memtable
        |> Memtable.stream(memtable_read_ahead)
        |> Stream.map(fn {key, value} ->
          {key, term_to_iovec(value)}
        end)

      {new_generations, to_delete} =
        merge(
          generations,
          Memtable.size(memtable),
          0,
          [],
          vencoded_memtable_stream,
          Path.join(data_dir, "disktables"),
          descriptor_pool,
          block_size_threshold,
          descriptor_pool_size
        )

      Memtable.clear(memtable)
      shadow_gentable = get_shadow_gentable(name)

      Gentable.clear(shadow_gentable)
      Gentable.insert(shadow_gentable, new_generations)

      swap_gentables(name)

      for {fd, filename, descriptor_pool_state} <- to_delete do
        :prim_file.close(fd)
        DescriptorPool.remove(descriptor_pool, descriptor_pool_state)
        :prim_file.delete(filename)
      end

      send(owner, {:microlsm_merge_done, merge_ref})
    end)

    merge_ref
  end

  defp finish_merge(state) do
    %{shadow_wal_fd: wal_fd} = state
    Wal.truncate(wal_fd)
    %{state | merge_ref: nil}
  end

  defp merge(
         [{generation, disktables} | generations],
         length,
         generation,
         to_delete,
         stream,
         disktables_dir,
         descriptor_pool,
         block_size_threshold,
         descriptor_pool_size
       ) do
    {disktable(descriptor_pool_state: descriptor_pool_state, filename: filename)} = disktables #FIXME
    {:ok, fd} = :prim_file.open(filename, [:read])
    rstream = Disktable.stream(fd)
    rlength = Disktable.total_length(fd) # TODO optimize
    stream = Microlsm.Stream.merge_streams(stream, rstream)

    to_delete = [{fd, filename, descriptor_pool_state} | to_delete]

    merge(
      generations,
      length + rlength,
      generation + 1,
      to_delete,
      stream,
      disktables_dir,
      descriptor_pool,
      block_size_threshold,
      descriptor_pool_size
    )
  end

  @encoded_deletes [term_to_binary({}), term_to_iovec({})]

  defp merge(generations, length, generation, to_delete, stream, disktables_dir, descriptor_pool, block_size_threshold, descriptor_pool_size) do
    postfix = "#{generation}_#{DateTime.to_unix DateTime.utc_now :millisecond}"
    filename = Path.join(disktables_dir, postfix)
    {:ok, fd} = :prim_file.open(filename, [:write, :read])

    {index, bloom_filter} =
      case generations do
        [] ->
          # We just drop tombstones in the oldest generation
          Stream.reject(stream, &match?({_, value} when value in @encoded_deletes, &1))

        _ ->
          stream
      end
      |> Disktable.write_stream(fd, length, block_size_threshold)

    :prim_file.close(fd)
    descriptor_pool_state = DescriptorPool.add(descriptor_pool, filename, descriptor_pool_size)

    disktable =
      disktable(
        generation: generation,
        bloom_filter: bloom_filter,
        index: index,
        filename: filename,
        descriptor_pool_state: descriptor_pool_state
      )

    {[{generation, {disktable}} | generations], to_delete}
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

  ## Compaction swap

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
    global_state(atomics_ref: atomics_ref) = :persistent_term.get({Microlsm, name})
    AtomicFlags.swap(atomics_ref, :memtables)
  end

  defp swap_gentables(name) do
    global_state(atomics_ref: atomics_ref) = :persistent_term.get({Microlsm, name})
    AtomicFlags.swap(atomics_ref, :gentables)
  end

  defp get_memtable_and_gentable(name) do
    global_state(
      atomics_ref: atomics_ref,
      memtable_read_ahead: memtable_read_ahead,
      memtable1: mt1,
      memtable2: mt2,
      gentable1: gt1,
      gentable2: gt2,
      descriptor_pool: descriptor_pool
    ) = :persistent_term.get({Microlsm, name})

    {memtable, shadow_memtable} = AtomicFlags.order(atomics_ref, :memtables, mt1, mt2)
    gentable = AtomicFlags.select(atomics_ref, :gentables, gt1, gt2)
    {memtable, shadow_memtable, gentable, descriptor_pool, memtable_read_ahead}
  end

  defp get_shadow_gentable(name) do
    global_state(atomics_ref: atomics_ref, gentable1: gt1, gentable2: gt2) = :persistent_term.get({Microlsm, name})
    AtomicFlags.select(atomics_ref, :gentables, gt2, gt1)
  end
end
