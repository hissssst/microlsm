defmodule Microlsm.FuzzTest do
  use ExUnit.Case
  import StreamData

  @moduletag fuzz: true, timeout: :infinity

  setup do
    {:ok, Microlsm.Test.Support.setup_datadir()}
  end

  defp x do
    one_of [
      integer(),
      bitstring(max_length: 1024),
      string(:ascii, max_length: 1024)
    ]
  end

  defp batch(key, value) do
    list_of(
      one_of([
        {:write, key, x()},
        {:delete, key},
      ]),
      max_length: 16
    )
  end

  defmodule ReferenceStore do
    def new(name) do
      :ets.new(name, [:ordered_set, :public, :named_table])
    end

    def read(name, key) do
      case :ets.lookup(name, key) do
        [{^key, value}] -> {:ok, value}
        _ -> :error
      end
    end

    def mread(name, keys) do
      Enum.reduce(keys, %{}, fn key, acc ->
        case :ets.lookup(name, key) do
          [{key, value}] -> Map.put(acc, key, value)
          _ -> acc
        end
      end)
    end

    def all(name) do
      :ets.tab2list(name)
    end

    def range_read(name, left_key, right_key) do
      selector = [{{:"$1", :"$2"}, [{:andalso, {:>=, :"$1", left_key}, {:"=<", :"$1", right_key}}], [{{:"$1", :"$2"}}]}]
      :ets.select(name, selector)
    end

    def write(name, key, value) do
      :ets.insert(name, {key, value})
      :ok
    end

    def write_nosync(name, key, value) do
      :ets.insert(name, {key, value})
      :ok
    end

    def delete(name, key) do
      :ets.delete(name, key)
      :ok
    end

    def batch(name, batch) do
      Enum.each(batch, fn
        {:write, key, value} -> write(name, key, value)
        {:delete, key} -> delete(name, key)
      end)
      :ok
    end
  end

  @tag skip: true
  test "Fuzz multi key", %{name: name, data_dir: data_dir} do
    test_length = 128 * 1024
    threshold = 1024
    kill_every = 250

    {:ok, _pid} =
      Microlsm.start_link(
        name: name,
        data_dir: data_dir,
        threshold: threshold
      )

    ReferenceStore.new(name)

    key = x()
    value = x()

    stream =
      frequency [
        {kill_every, fixed_list([:write, name, key, value])},
        {kill_every, fixed_list([:delete, name, key])},
        {kill_every, fixed_list([:batch, name, batch(key, value)])},
        {3, :kill}
      ]

    import StreamData, only: []

    ops =
      stream
      |> Stream.take(test_length)
      |> Enum.reduce([], fn entry, acc ->
        case entry do
          [op | args] ->
            apply(ReferenceStore, op, args)
            apply(Microlsm, op, args)

          :kill ->
            pid = Process.whereis(name)
            Process.unlink(pid)

            Process.exit(pid, :kill)
            await_killed(pid)

            {:ok, _} =
              Microlsm.start_link(
                name: name,
                data_dir: data_dir,
                threshold: threshold
              )

            check(name, [:kill | acc])
        end

        [entry | acc]
      end)

    check(name, ops)
  end

  def check(name, ops) do
    ops = :lists.reverse(ops)

    left = Microlsm.all(name)
    right = ReferenceStore.all(name)

    ml = MapSet.new(left)
    mr = MapSet.new(right)

    i = MapSet.intersection(ml, mr)

    dl = MapSet.difference(ml, i)
    dr = MapSet.difference(mr, i)

    kdl = MapSet.new(Stream.map(dl, fn {key, _} -> key end))
    kdr = MapSet.new(Stream.map(dr, fn {key, _} -> key end))

    kdi = MapSet.intersection(kdl, kdr)
    kol = MapSet.difference(kdl, kdi)
    kor = MapSet.difference(kdr, kdi)

    for key <- kdi do
      IO.puts """
      Different value
        key:        #{inspect(key)}
        reference:  #{inspect(ReferenceStore.read(name, key))}
        value:      #{inspect(Microlsm.read(name, key))}
        location:   #{inspect(Microlsm.location(name, key))}
        operations: #{inspect(filter_ops(ops, key, 10))}
      """
    end

    for key <- kol do
      IO.puts """
      Must not be present
        key:        #{inspect(key)}
        reference:  #{inspect(ReferenceStore.read(name, key))}
        value:      #{inspect(Microlsm.read(name, key))}
        location:   #{inspect(Microlsm.location(name, key))}
        operations: #{inspect(filter_ops(ops, key, 10))}
      """
    end

    for key <- kor do
      IO.puts """
      Must be present
        key:        #{inspect(key)}
        reference:  #{inspect(ReferenceStore.read(name, key))}
        value:      #{inspect(Microlsm.read(name, key))}
        location:   #{inspect(Microlsm.location(name, key))}
        operations: #{inspect(filter_ops(ops, key, 10))}
      """
    end

    assert ml == mr, "Different"
  end

  defp await_killed(pid) do
    if Process.alive?(pid) do
      Process.sleep(1)
      await_killed(pid)
    end
  end

  defp filter_ops(_ops, _key, 0) do
    []
  end

  defp filter_ops([[:write, _, key, v] | ops], key, len) do
    [{:write, v} | filter_ops(ops, key, len - 1)]
  end

  defp filter_ops([[:delete, _, key] | ops], key, len) do
    [:delete | filter_ops(ops, key, len - 1)]
  end

  defp filter_ops([[:batch, _, batch] | ops], key, len) do
    case filter_batch(batch, key) do
      [] -> filter_ops(ops, key, len)
      batch -> [{:batch, batch} | filter_ops(ops, key, len - 1)]
    end
  end

  defp filter_ops([:kill | ops], key, len) do
    ops = filter_ops(ops, key, len - 1)
    case ops do
      [:kill | _] -> ops
      ops -> [:kill | ops]
    end
  end

  defp filter_ops([_ | ops], key, len) do
    filter_ops(ops, key, len)
  end

  defp filter_ops([], _key, _len) do
    []
  end

  defp filter_batch([{:write, key, v} | batch], key) do
    [{:write, v}| filter_batch(batch, key)]
  end

  defp filter_batch([{:delete, key} | batch], key) do
    [:delete | filter_batch(batch, key)]
  end

  defp filter_batch([_ | batch], key) do
    filter_batch(batch, key)
  end

  defp filter_batch([], _key) do
    []
  end

  # @tag skip: true
  test "Fuzz single key", %{name: name, data_dir: data_dir} do
    test_length = 128 * 1024
    threshold = 1024
    kill_every = 512

    {:ok, _pid} =
      Microlsm.start_link(
        name: name,
        data_dir: data_dir,
        threshold: threshold,
        wal_length_threshold: 10
      )

    key = :x
    first_value = :y
    ReferenceStore.new(name)
    ReferenceStore.write(name, key, first_value)
    Microlsm.write(name, key, first_value)

    import StreamData

    x =
      one_of [
        integer(),
        bitstring(max_length: 1024),
        string(:ascii, max_length: 1024)
      ]

    stream =
      frequency [
        {kill_every, x},
        {kill_every, :all},
        {2, :kill}
      ]

    import StreamData, only: []

    IO.inspect data_dir

    {_, ops} =
      stream
      |> Stream.take(test_length)
      |> Enum.reduce({first_value, []}, fn entry, {old_value, acc} ->
        case entry do
          :kill ->
            pid = Process.whereis(name)
            Process.unlink(pid)

            Process.exit(pid, :kill)
            await_killed(pid)

            {:ok, _} =
              Microlsm.start_link(
                name: name,
                data_dir: data_dir,
                reset_counters: false,
                threshold: threshold
              )

            acc = [:kill | acc]
            check(name, acc)
            {old_value, acc}

          :all ->
            assert Enum.to_list(Microlsm.all(name)) == ReferenceStore.all(name)
            {old_value, acc}

          value ->
            assert {:ok, ^old_value} = ReferenceStore.read(name, key)
            assert {:ok, ^old_value} = Microlsm.read(name, key)

            ReferenceStore.write(name, key, value)
            Microlsm.write(name, key, value)

            assert {:ok, ^value} = Microlsm.read(name, key)
            assert {:ok, ^value} = ReferenceStore.read(name, key)

            {value, [[:write, name, key, value] | acc]}
        end
      end)

    check(name, ops)
    Microlsm.Stats.print(name)
  end

  test "Range reads", %{name: name, data_dir: data_dir} do
    test_length = 128 * 1024
    threshold = 1024
    kill_every = 512

    key = integer()
    value = x()
    batch = batch(key, value)

    {:ok, _pid} =
      Microlsm.start_link(
        name: name,
        data_dir: data_dir,
        threshold: 1024
      )

    ReferenceStore.new(name)

    x =
      one_of [
        integer(),
        bitstring(max_length: 1024),
        string(:ascii, max_length: 1024)
      ]

    stream =
      frequency [
        {1 * kill_every, fixed_list([:write, name, key, value])},
        {4, :kill},
        {1 * kill_every, fixed_list([:delete, name, key])},
        # {10 * kill_every, fixed_list([:batch, name, batch])},
        {1 * kill_every, fixed_list([:range_read, name, key, key])},
        {1 * kill_every, fixed_list([:all, name])}
      ]

    IO.inspect data_dir

    start_watcher(name)

    ops =
      stream
      |> Stream.take(test_length)
      |> Stream.map(fn
        [:range_read, name, k1, k2] when k1 > k2 ->
          [:range_read, name, k2, k1]

        other ->
          other
      end)
      |> Enum.reduce([], fn entry, acc ->
        case entry do
          :kill ->
            pid = Process.whereis(name)
            Process.unlink(pid)

            Process.exit(pid, :kill)
            await_killed(pid)

            {:ok, _} =
              Microlsm.start_link(
                name: name,
                data_dir: data_dir,
                reset_counters: false,
                threshold: threshold
              )

            check(name, acc)

          [op | args] when op in [:all, :range_read] ->
            reference = Enum.to_list(apply(ReferenceStore, op, args))
            microlsm = Enum.to_list(apply(Microlsm, op, args))

            keys =
              for {key, _} <- reference do
                key
              end

            assert ReferenceStore.mread(name, keys) == Microlsm.mread(name, keys)

            with [_, _ | _] = diff <- List.myers_difference(reference, microlsm) do
              diff_keys =
                diff
                |> Enum.reject(&match?({:eq, _}, &1))
                |> Enum.flat_map(fn {_, pairs} -> Enum.map(pairs, fn {key, _value} -> key end) end)
                |> Enum.into(MapSet.new())

              IO.inspect entry, label: :operation

              for key <- diff_keys do
                loc = Microlsm.location(name, key)
                IO.inspect key, label: :key
                IO.inspect Enum.find(reference, &match?({^key, _}, &1)), label: :reference
                IO.inspect Enum.find(microlsm, &match?({^key, _}, &1)), label: :microlsm
                IO.inspect Enum.find(apply(Microlsm, op, args), &match?({^key, _}, &1)), label: :microlsm_retry
                IO.inspect filter_ops(acc, key, 5), label: :ops
                IO.inspect loc, label: :location
                IO.puts ""
              end

              assert false
            end

          [op | args] ->
            assert apply(ReferenceStore, op, args) == apply(Microlsm, op, args)
        end

        [entry | acc]
      end)

    check(name, ops)
    Microlsm.Stats.print(name)
  end

  defp start_watcher(name) do
    owner = self()
    spawn(fn -> watcher_loop(owner, name) end)
  end

  defp watcher_loop(owner, name) do
    Process.sleep(5_000)
    watcher_loop(owner, name)
  end
end
