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
    memtable_read_ahead: nil

  require AtomicFlags
  require ODCounter

  @call_timeout 5_000

  @typedoc """
  A key. Any Elixir term can be used as a key. Please keep in mind, that runtime-specific terms
  like `t:pid/0` and `t:reference/0` are encoded with current node ID and will represent the different
  node when the runtime restarts.
  """
  @type key :: any()

  @typedoc """
  A value. Any Elixir term can be used as a value. Please keep in mind, that runtime-specific terms
  like `t:pid/0` and `t:reference/0` are encoded with current node ID and will represent the different
  node when the runtime restarts.
  """
  @type value :: any()

  @typedoc """
  Name of the table. It must be an atom. Table server is registered under this name.
  """
  @type t :: atom()

  @typedoc """
  Options passed in the `start_link/2`

  * `name` (`t:name/0`) Required. — Name of the table.

  * `data_dir` (`t:Path.t/0`) Required. — A directory which will be used to store the data of the table.

  * `descriptor_pool_size` (`t:pos_integer/0`) — A size of pool of open file descriptors for a single file. Defaults to `2`.

  * `threshold` (bytes) — A maximum amount of data to be kept in memory table. Exceeding this threshold triggers a compaction. Defaults to 16 megabytes.

  * `wal_length_threshold` (length number) — A maximum length of WAL log to be held on disk. Exceeding this threshold triggers a compaction. Defaults to 10k.

  * `block_size_threshold` (bytes) — A maximum size of a block in a disk table. Defaults to 4 kylobytes.

  * `max_batch_length` (`t:pos_integer/0`) — A maximum size of a batch of entries before performing dump to WAL. Defaults to `10000`.

  * `memtable_read_ahead` (`t:pos_integer/0`) — An amount of entries to read from memtable in one call. Defaults to `1000`.
  """
  @type start_option ::
          {:name, t()}
          | {:allow_overflow, boolean()}
          | {:data_dir, Path.t()}
          | {:descriptor_pool_size, pos_integer()}
          | {:threshold, bytes :: pos_integer()}
          | {:wal_length_threshold, length :: pos_integer()}
          | {:block_size_threshold, bytes :: pos_integer()}
          | {:max_batch_length, pos_integer()}
          | {:memtable_read_ahead, pos_integer()}

  @typedoc """
  An operation passed in a list to `batch/3` call.
  """
  @type batch_operation ::
          {:delete, key()}
          | {:write, key(), value()}

  @typedoc """
  Options passed in the functions which call the Microlsm server.

  * `timeout` (`t:timeout/0`) — Timeout for the call. Defaults to `5000`
  """
  @type call_timeout_option :: {:timeout, timeout()}

  import Microlsm.Debug

  ## Safety checks

  defmacrop assert_alive!(name) do
    quote do
      name = unquote(name)
      with nil <- Process.whereis(name) do
        raise ArgumentError, "Table #{name} does not exist"
      end
    end
  end

  ## Public API

  @doc """
  Starts a key-value table. Table is not available for reads until this function returns.

  See `t:start_option/0` for documentation of the options.
  """
  @spec start_link([start_option()]) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    case Process.whereis(name) do
      nil ->
        GenServer.start_link(Microlsm, opts)

      pid ->
        {:error, {:already_started, pid}}
    end
  end

  @doc """
  Cleans all the existing data in the table. Table is
  ready for use right after this call.

  ## Example

      iex> Microlsm.write(:my_table, [:user, 1, :name], "Username")
      iex> Microlsm.read(:my_table, [:user, 1, :name])
      {:ok, "Username"}
      iex> Microlsm.clean(:my_table)
      iex> Microlsm.read(:my_table, [:user, 1, :name])
      :error
  """
  @spec clean(t(), [call_timeout_option()]) :: :ok
  def clean(name, opts \\ []) do
    GenServer.call(name, :clean, call_timeout(opts))
  end

  @doc """
  Writes a value to the given key. If the key already exists, value is overwritten.
  Returns when write is persisted.

  ## Example

      iex> Microlsm.write(:my_table, [:user, 1, :name], "Username")
      iex> Microlsm.read(:my_table, [:user, 1, :name])
      {:ok, "Username"}
  """
  @spec write(t(), key(), value(), [call_timeout_option()]) :: :ok
  def write(name, key, value, opts \\ []) do
    GenServer.call(name, {:write, key, {value}}, call_timeout(opts))
  end

  @doc """
  Writes a value to the given key. If the key already exists, value is overwritten.
  Returns when write is visible, but not yet persisted.

  ## Example

      iex> Microlsm.write_nosync(:my_table, [:user, 1, :name], "Username")
      iex> Microlsm.read(:my_table, [:user, 1, :name])
      {:ok, "Username"}
  """
  @spec write_nosync(t(), key(), value(), [call_timeout_option()]) :: :ok
  def write_nosync(name, key, value, opts \\ []) do
    GenServer.call(name, {:write_nosync, key, {value}}, call_timeout(opts))
  end

  @doc """
  Writes a value to the given key. If the key already exists, value is overwritten.
  Returns immediately, when write is not even visible.
  """
  @spec async_write(t(), key(), value()) :: :ok
  def async_write(name, key, value) do
    GenServer.cast(name, {:async_write, key, {value}})
    :ok
  end

  @doc """
  Deletes a key. Succeeds even when key doesn't exist.
  Returns when delete is persisted.

  ## Example

      iex> Microlsm.write(:my_table, [:user, 1, :name], "Username")
      iex> Microlsm.read(:my_table, [:user, 1, :name])
      {:ok, "Username"}
      iex> Microlsm.delete(:my_table, [:user, 1, :name])
      iex> Microlsm.read(:my_table, [:user, 1, :name])
      :error
  """
  @spec delete(t(), key(), [call_timeout_option()]) :: :ok
  def delete(name, key, opts \\ []) do
    GenServer.call(name, {:write, key, {}}, call_timeout(opts))
  end

  @doc """
  Deletes a key. Succeeds even when key doesn't exist.
  Returns when delete is visible, but not yet persisted.

  ## Example

      iex> Microlsm.write(:my_table, [:user, 1, :name], "Username")
      iex> Microlsm.read(:my_table, [:user, 1, :name])
      {:ok, "Username"}
      iex> Microlsm.delete_nosync(:my_table, [:user, 1, :name])
      iex> Microlsm.read(:my_table, [:user, 1, :name])
      :error
  """
  @spec delete_nosync(t(), key(), [call_timeout_option()]) :: :ok
  def delete_nosync(name, key, opts \\ []) do
    GenServer.call(name, {:write_nosync, key, {}}, call_timeout(opts))
  end

  @doc """
  Deletes a key. Succeeds even when key doesn't exist.
  Returns immediately, when delete is not even visible.
  """
  @spec async_delete(t(), key()) :: :ok
  def async_delete(name, key) do
    GenServer.cast(name, {:async_write, key, {}})
    :ok
  end

  @doc """
  Atomically performs a batch of operations.
  Returns when all changes are persisted.

  ## Example

      iex> Microlsm.batch(:my_table, [
      ...>   {:write, :key1, :value1},
      ...>   {:write, :key2, :value2}
      ...> ])
      iex> Microlsm.read(:my_table, :key1)
      {:ok, :value1}
  """
  @spec batch(t(), Enumerable.t(batch_operation()), [call_timeout_option()]) :: :ok
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

  @doc """
  Reads a value for the given key. Returns `:error` when value is not found

  ## Example

      iex> Microlsm.write(:my_table, [:user, 1, :name], "Username")
      iex> Microlsm.read(:my_table, [:user, 1, :name])
      {:ok, "Username"}
  """
  @spec read(t(), key()) :: {:ok, value()} | :error
  def read(name, key) do
    #   :eflambe.apply({Microlsm, :do_read, [name, key]}, output_directory: ~c"flamegraphs")
    # end
    # def do_read(name, key) do
    assert_alive!(name)

    global_state(
      atomics_ref: atomics_ref,
      memtable1: mt1,
      memtable2: mt2,
      gentable1: gt1,
      gentable2: gt2,
      descriptor_pool: descriptor_pool
    ) = :persistent_term.get({Microlsm, name})

    AtomicFlags.switch(atomics_ref, :reading) do
      {memtable, shadow_memtable} = AtomicFlags.order(atomics_ref, :memtables, mt1, mt2)

      result =
        with(
          :error <- Memtable.read(memtable, key),
          :error <- Memtable.read(shadow_memtable, key)
        ) do
          ODCounter.add(Microlsm, name, :memtable_miss)
          gentable = AtomicFlags.select(atomics_ref, :gentables, gt1, gt2)
          generations = Gentable.to_list(gentable)
          disktable_read(generations, key, descriptor_pool, name)
        else
          result ->
            ODCounter.add(Microlsm, name, :memtable_hit)
            result
        end

      case result do
        {:ok, {value}} -> {:ok, value}
        _ -> :error
      end
    else
      :error
    end
  end

  defp disktable_read([], _key, _descriptor_pool, name) do
    ODCounter.add(Microlsm, name, :complete_miss)
    :error
  end

  defp disktable_read([{_generation, disktables} | rest], key, descriptor_pool, name) do
    case Gentable.search_disktable_in_generation(disktables, key) do
      disktable(
        max_block_size: max_block_size,
        bloom_filter: bloom_filter,
        descriptor_pool_state: descriptor_pool_state,
        index: index
      ) ->
        ODCounter.add(Microlsm, name, :generation_hit)
        case BloomFilter.check(bloom_filter, key) do
          :no ->
            ODCounter.add(Microlsm, name, :bloom_filter_miss)
            disktable_read(rest, key, descriptor_pool, name)

          :maybe ->
            ODCounter.add(Microlsm, name, :bloom_filter_hit)
            result = Disktable.find_block(index, key)

            DescriptorPool.checkout(descriptor_pool, descriptor_pool_state, fn fd ->
              Disktable.find_value(fd, result, key, max_block_size)
            end)
            |> case do
              {:ok, value} ->
                ODCounter.add(Microlsm, name, :disktable_hit)
                {:ok, value}

              {:error, _} ->
                ODCounter.add(Microlsm, name, :disktable_miss)
                disktable_read(rest, key, descriptor_pool, name)
            end
        end

      :not_found ->
        ODCounter.add(Microlsm, name, :generation_miss)
        disktable_read(rest, key, descriptor_pool, name)
    end
  end

  @doc """
  Returns an ordered lazily evaluated Enumerable of all current key-value pairs.
  Returned stream is consistent and returns all data present in the Microlsm key
  range **at the moment of the call**. Be aware that it instantiates all memtable
  entries as soon as it is called.

  Returned stream MUST be traversed or closed. Otherwise zombie file descriptors will be left open.

  ## Example

      iex> Microlsm.batch(:my_table, [
      ...>   {:write, :key1, :value1},
      ...>   {:write, :key2, :value2}
      ...> ])
      iex> Enum.to_list Microlsm.all(:my_table)
      [{:key1, :value1}, {:key2, :value2}]
  """
  @spec all(t()) :: Enumerable.t({key(), value()})
  def all(name) do
    assert_alive!(name)
    global_state(atomics_ref: atomics_ref) = :persistent_term.get({Microlsm, name})

    AtomicFlags.switch(atomics_ref, :reading) do
      {memtable, shadow_memtable, gentable, _descriptor_pool, memtable_read_ahead} = get_memtable_and_gentable(name)
      generations = Gentable.to_list(gentable)

      memtable_stream = Memtable.stream(memtable, memtable_read_ahead)
      shadow_memtable_stream = Memtable.stream(shadow_memtable, memtable_read_ahead)

      all_memtables_stream = Microlsm.Stream.merge_streams(memtable_stream, shadow_memtable_stream)

      # To avoid funny inconsistencies
      all_memtables_list = Enum.to_list(all_memtables_stream)

      {fds, stream} =
        Enum.reduce(generations, {[], all_memtables_list}, fn {_, {disktable(filename: filename, index: index)}}, {fds, stream} ->
          # IO.inspect filename, label: :filename
          # IO.inspect generations, label: :generations
          {:ok, fd} =
            with {:error, :enoent} <- :prim_file.open(filename, [:read]) do
              for fd <- fds, do: :prim_file.close(fd)
              throw :retry
            end

          disk_stream = Disktable.stream(fd, index)
          stream = Microlsm.Stream.merge_streams(stream, disk_stream)
          {[fd | fds], stream}
        end)

      finalize_read_stream(stream, fds)
    else
      []
    end
  catch
    :retry ->
      ODCounter.add(Microlsm, name, :mread_retries)
      all(name)
  end

  @doc """
  Returns an ordered lazily evaluated Enumerable of all key-value pairs present in the
  specified key range, including left and right boundaries. Returned stream is consistent
  and returns all data present in the Microlsm key range **at the moment of the call**.
  Be aware that it instantiates all in-range memtable entries as soon as it is called.

  Returned stream MUST be traversed or closed. Otherwise zombie file descriptors will be left open.

  ## Example

      iex> Microlsm.batch(:my_table, [
      ...>   {:write, :key1, :value1},
      ...>   {:write, :key2, :value2},
      ...>   {:write, :key3, :value3},
      ...>   {:write, :key4, :value4},
      ...>   {:write, :key5, :value5}
      ...> ])
      iex> Enum.to_list Microlsm.range_read(:my_table, :key2, :key4)
      [{:key2, :value2}, {:key3, :value3}, {:key4, :value4}]
  """
  @spec range_read(t(), key(), key()) :: Enumerable.t({key(), value()})
  def range_read(name, left_key, right_key) when left_key <= right_key do
    assert_alive!(name)
    global_state(atomics_ref: atomics_ref) = :persistent_term.get({Microlsm, name})

    AtomicFlags.switch(atomics_ref, :reading) do
      {memtable, shadow_memtable, gentable, _descriptor_pool, memtable_read_ahead} = get_memtable_and_gentable(name)
      generations = Gentable.to_list(gentable)
      memtable_stream = Memtable.range_read(memtable, left_key, right_key, memtable_read_ahead)
      shadow_memtable_stream = Memtable.range_read(shadow_memtable, left_key, right_key, memtable_read_ahead)

      all_memtables_stream = Microlsm.Stream.merge_streams(memtable_stream, shadow_memtable_stream)

      # To avoid funny inconsistencies
      all_memtables_list = Enum.to_list(all_memtables_stream)

      {fds, stream} =
        Enum.reduce(generations, {[], all_memtables_list}, fn {_, {disktable(filename: filename, index: index)}}, {fds, stream} ->
          case Disktable.range_offsets(index, left_key, right_key) do
            [] ->
              {fds, stream}

            offsets ->
              {:ok, fd} =
                with {:error, :enoent} <- :prim_file.open(filename, [:read]) do
                  for fd <- fds, do: :prim_file.close(fd)
                  throw :retry
                end

              disk_stream = Disktable.range_stream(fd, left_key, right_key, offsets)
              stream = Microlsm.Stream.merge_streams(stream, disk_stream)
              {[fd | fds], stream}
          end
        end)

      finalize_read_stream(stream, fds)
    else
      []
    end
  catch
    :retry ->
      ODCounter.add(Microlsm, name, :mread_retries)
      all(name)
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

  @doc false
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

    disktable(
      max_block_size: max_block_size,
      bloom_filter: bloom_filter,
      filename: filename,
      generation: generation,
      index: index
    ) = disktable

    {:ok, fd} = :prim_file.open(filename, [:read])

    bloom_check = BloomFilter.check(bloom_filter, key)
    try do
      block = Disktable.find_block(index, key)
      Disktable.find_value(fd, block, key, max_block_size)
    else
      {:ok, value} -> {:disktable, generation, bloom_check, value, filename}
      {:error, _} -> disktable_location(rest, key)
    after
      :prim_file.close(fd)
    end
  end

  @doc false
  def debug(name) do
    pid = Process.whereis(name)
    {_, _, gentable, _descriptor_pool, _memtable_read_ahead} = get_memtable_and_gentable(name)
    generations = Gentable.to_list(gentable)
    state = :sys.get_state(pid)
    state = Map.put(state, :generations, generations)
    Map.put(state, :dlog, get_dlog(pid))
  end

  ## GenServer

  defp recover_global_state(name, memtable_read_ahead) do
    global_state = create_global_state(memtable_read_ahead)
    :persistent_term.put({Microlsm, name}, global_state)
    global_state
  end

  defp create_global_state(memtable_read_ahead) do
    atomics_ref = AtomicFlags.new()
    memtable = Memtable.new()
    shadow_memtable = Memtable.new()
    gentable = Gentable.new()
    shadow_gentable = Gentable.new()
    descriptor_pool = DescriptorPool.new()

    global_state(
      atomics_ref: atomics_ref,
      memtable1: memtable,
      memtable2: shadow_memtable,
      gentable1: gentable,
      gentable2: shadow_gentable,
      descriptor_pool: descriptor_pool,
      memtable_read_ahead: memtable_read_ahead
    )
  end

  @doc false
  def init(opts) do
    name = Keyword.fetch!(opts, :name)

    ODCounter.init_schema(Microlsm, runtime_size: 16, ignore_if_exists: true)
    ODCounter.new(Microlsm, name, ignore_if_exists: true)
    ODCounter.reset(Microlsm, name)

    data_dir = Path.expand(Keyword.fetch!(opts, :data_dir))
    check_or_write_lock(data_dir)
    disktable_dir = Path.join(data_dir, "disktables")

    allow_overflow = Keyword.get(opts, :allow_overflow, false)
    block_size_threshold = Keyword.get(opts, :block_size_threshold, 4 * 1024)
    memtable_read_ahead = Keyword.get(opts, :memtable_read_ahead, 1000)
    batch_timeout = Keyword.get(opts, :batch_timeout, 0)
    threshold = Keyword.get(opts, :threshold, 16 * 1024 * 1024)
    max_batch_length = Keyword.get(opts, :max_batch_length, 10_000)
    descriptor_pool_size = Keyword.get(opts, :descriptor_pool_size, 2)
    wal_length_threshold = Keyword.get(opts, :wal_length_threshold, div(threshold, 32) + 10)

    wal_dir = Path.join(data_dir, "wal")
    File.mkdir_p!(wal_dir)

    global_state(
      memtable1: memtable,
      memtable2: shadow_memtable,
      gentable1: gentable,
      descriptor_pool: descriptor_pool
    ) = recover_global_state(name, memtable_read_ahead)

    {wal, shadow_wal, wal_swap_flag_fd} = restore_wals(memtable, shadow_memtable, wal_dir)

    with :ok <- File.mkdir_p(disktable_dir) do
      dlog(:mkdir_p_disktable_dir)
    end

    restore_disktables(disktable_dir, descriptor_pool, gentable, descriptor_pool_size)

    state = %{
      merge_ref: nil,
      name: name,
      data_dir: data_dir,

      allow_overflow: allow_overflow,
      block_size_threshold: block_size_threshold,
      batch_timeout: batch_timeout,
      threshold: threshold,
      max_batch_length: max_batch_length,
      descriptor_pool_size: descriptor_pool_size,
      wal_length_threshold: wal_length_threshold,

      wal: wal,
      shadow_wal: shadow_wal,
      wal_swap_flag_fd: wal_swap_flag_fd,

      batch_length: 0,
      batch: [],
      reply_tos: []
    }

    Process.register(self(), name)
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

          {length, max_block_size, index} ->
            bloom_filter = build_bloom_filter(fd, index, length)
            descriptor_pool_state = DescriptorPool.add(descriptor_pool, filename, descriptor_pool_size)

            entry =
              disktable(
                id: id,
                generation: generation,
                bloom_filter: bloom_filter,
                index: index,
                filename: filename,
                descriptor_pool_state: descriptor_pool_state,
                length: length,
                max_block_size: max_block_size
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
    wal =
      Wal.open(full_wal_path, fn {_length, batch} ->
        Memtable.write(memtable, batch)
      end)

    wal
  end

  @doc false
  def handle_continue(:init_dump, state) do
    dlog(:continue_init_dump)
    %{data_dir: data_dir, name: name, block_size_threshold: block_size_threshold, descriptor_pool_size: descriptor_pool_size} = state

    {_memtable, shadow_memtable, gentable, descriptor_pool, memtable_read_ahead} = get_memtable_and_gentable(name)

    if Memtable.empty?(shadow_memtable) do
      {:noreply, state}
    else
      merge_ref = spawn_merge(gentable, shadow_memtable, data_dir, name, descriptor_pool, block_size_threshold, descriptor_pool_size, memtable_read_ahead)
      {:noreply, %{state | merge_ref: merge_ref}}
    end
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

  @doc false
  def handle_call(:clean, _from, state) do
    %{name: name, wal: working_wal, shadow_wal: shadow_wal} = state

    state =
      with %{merge_ref: merge_ref} when not is_nil(merge_ref) <- state do
        ODCounter.add(Microlsm, name, :dumps_late)
        await_merge_finish(merge_ref, state)
      end

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

    :erlang.garbage_collect()
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

  @doc false
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

  @doc false
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
      wal: wal,
      batch: batch,
      batch_length: batch_length,
      allow_overflow: allow_overflow,
      reply_tos: reply_tos,
      threshold: threshold,
      wal_length_threshold: wal_length_threshold
    } = state

    {memtable, _, _, _, _} = get_memtable_and_gentable(name)

    batch = :lists.reverse(batch)
    wal = Wal.push_batch(wal, batch, batch_length)
    Memtable.write(memtable, batch)

    state = %{state | wal: wal}

    state =
      if Wal.length(wal) >= wal_length_threshold or Memtable.memory(memtable) >= threshold do
        case state do
          %{merge_ref: nil} ->
            start_dump(state)

          %{merge_ref: merge_ref} ->
            ODCounter.add(Microlsm, name, :dumps_late)
            if allow_overflow do
              state
            else
              await_merge_finish(merge_ref, state)
            end
        end
      else
        state
      end

    mreply(reply_tos)

    %{state | batch: [], reply_tos: [], batch_length: 0}
  end

  defp await_merge_finish(merge_ref, state) when not is_nil(merge_ref) do
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

    state = swap_wals(state)
    swap_memtables(name)

    merge_ref = spawn_merge(gentable, memtable, data_dir, name, descriptor_pool, block_size_threshold, descriptor_pool_size, memtable_read_ahead)

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
          Memtable.length(memtable),
          [],
          0,
          [],
          vencoded_memtable_stream,
          Path.join(data_dir, "disktables"),
          descriptor_pool,
          block_size_threshold,
          descriptor_pool_size
        )

      shadow_gentable = get_shadow_gentable(name)
      Gentable.clear(shadow_gentable)
      Gentable.insert(shadow_gentable, new_generations)

      swap_gentables(name)
      Memtable.clear(memtable)

      for {fd, filename, descriptor_pool_state} <- to_delete do
        # IO.inspect filename, label: :deleting
        :prim_file.close(fd)
        DescriptorPool.remove(descriptor_pool, descriptor_pool_state)
        :prim_file.delete(filename)
      end

      send(owner, {:microlsm_merge_done, merge_ref})
    end)

    merge_ref
  end

  defp finish_merge(state) do
    %{shadow_wal: shadow_wal} = state
    shadow_wal = Wal.truncate(shadow_wal)
    %{state | shadow_wal: shadow_wal, merge_ref: nil}
  end

  @encoded_deletes [term_to_iovec({}), term_to_binary({})]

  defp merge(
         generations,
         total_length,
         lengths,
         generation,
         to_delete,
         stream,
         disktable_dir,
         descriptor_pool,
         block_size_threshold,
         descriptor_pool_size
       ) do
    case generations do
      [{^generation, disktables} | generations] ->
        {disktable(descriptor_pool_state: descriptor_pool_state, filename: filename)} = disktables #FIXME
        {:ok, fd} = :prim_file.open(filename, [:read])
        {rlength, _, rstream} = Disktable.header_and_stream(fd)
        stream = Microlsm.Stream.merge_streams(stream, rstream)

        to_delete = [{fd, filename, descriptor_pool_state} | to_delete]

        merge(
          generations,
          total_length + rlength,
          [rlength | lengths],
          generation + 1,
          to_delete,
          stream,
          disktable_dir,
          descriptor_pool,
          block_size_threshold,
          descriptor_pool_size
        )

      _ ->
        write_full_filename = filename_for_generation(disktable_dir, generation)
        {:ok, fd} = :prim_file.open(write_full_filename, [:write, :read])

        {index, bloom_filter, max_block_size, table_length} =
          case generations do
            [] ->
              # We just drop tombstones in the oldest generation
              Stream.reject(stream, &match?({_, value} when value in @encoded_deletes, &1))

            _ ->
              stream
          end
          |> Disktable.write_stream(fd, total_length, block_size_threshold)

        :prim_file.close(fd)
        backtracked_generation = backtrack_generation(lengths, table_length, generation)

        full_filename =
          if backtracked_generation != generation do
            full_filename = filename_for_generation(disktable_dir, backtracked_generation)
            # IO.inspect {write_full_filename, full_filename}, label: :renaming
            :ok = :prim_file.rename(write_full_filename, full_filename)
            full_filename
          else
            write_full_filename
          end

        descriptor_pool_state = DescriptorPool.add(descriptor_pool, full_filename, descriptor_pool_size)

        disktable =
          disktable(
            generation: backtracked_generation,
            bloom_filter: bloom_filter,
            index: index,
            filename: full_filename,
            descriptor_pool_state: descriptor_pool_state,
            length: table_length,
            max_block_size: max_block_size
          )

        {[{backtracked_generation, {disktable}} | generations], to_delete}
    end
  end

  defp backtrack_generation([higher | lengths], table_length, generation) when higher >= table_length do
    backtrack_generation(lengths, table_length, generation - 1)
  end

  defp backtrack_generation(_, _, generation) do
    generation
  end

  defp filename_for_generation(disktable_dir, generation) do
    postfix = DateTime.to_unix(DateTime.utc_now(:millisecond), :millisecond) #FIXME need proper postfix generator
    fname = "#{generation}_#{postfix}"
    Path.join(disktable_dir, fname)
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

  ## Write lock

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
    %{wal: working_wal, shadow_wal: shadow_wal, wal_swap_flag_fd: wal_swap_flag_fd} = state

    to_write =
      case :prim_file.pread(wal_swap_flag_fd, 0, 8) do
        {:ok, "0"} -> "1"
        {:ok, "1"} -> "0"
      end

    :prim_file.pwrite(wal_swap_flag_fd, 0, to_write)
    :prim_file.sync(wal_swap_flag_fd)

    %{state | wal: shadow_wal, shadow_wal: working_wal}
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
