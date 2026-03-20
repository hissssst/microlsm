defmodule Microlsm.FuzzTest do
  use ExUnit.Case

  @moduletag fuzz: true, timeout: :infinity

  setup do
    {:ok, Microlsm.Test.Support.setup_datadir()}
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

    def all(name) do
      :ets.tab2list(name)
    end

    def write(name, key, value) do
      :ets.insert(name, {key, value})
    end

    def write_nosync(name, key, value) do
      :ets.insert(name, {key, value})
    end

    def delete(name, key) do
      :ets.delete(name, key)
    end

    def batch(name, batch) do
      Enum.each(batch, fn
        {:write, key, value} -> write(name, key, value)
        {:delete, key} -> delete(name, key)
      end)
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

    import StreamData

    x =
      one_of [
        integer(),
        bitstring(max_length: 1024),
        string(:ascii, max_length: 1024)
      ]

    key = integer()

    value = x

    batch =
      list_of(
        one_of([
          {:write, key, value},
          {:delete, key},
        ]),
        max_length: 16
      )

    stream =
      frequency [
        {kill_every, fixed_list([:write, name, key, value])},
        {kill_every, fixed_list([:delete, name, key])},
        {kill_every, fixed_list([:batch, name, batch])},
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
        operations: #{inspect(filter_ops(ops, key))}
      """
    end

    for key <- kol do
      IO.puts """
      Must not be present
        key:        #{inspect(key)}
        reference:  #{inspect(ReferenceStore.read(name, key))}
        value:      #{inspect(Microlsm.read(name, key))}
        location:   #{inspect(Microlsm.location(name, key))}
        operations: #{inspect(filter_ops(ops, key))}
      """
    end

    for key <- kor do
      IO.puts """
      Must be present
        key:        #{inspect(key)}
        reference:  #{inspect(ReferenceStore.read(name, key))}
        value:      #{inspect(Microlsm.read(name, key))}
        location:   #{inspect(Microlsm.location(name, key))}
        operations: #{inspect(filter_ops(ops, key))}
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

  defp filter_ops([[:write, _, key, v] | ops], key) do
    [{:write, v} | filter_ops(ops, key)]
  end

  defp filter_ops([[:delete, _, key] | ops], key) do
    [:delete | filter_ops(ops, key)]
  end

  defp filter_ops([[:batch, _, batch] | ops], key) do
    case filter_batch(batch, key) do
      [] -> filter_ops(ops, key)
      batch -> [{:batch, batch} | filter_ops(ops, key)]
    end
  end

  defp filter_ops([_ | ops], key) do
    filter_ops(ops, key)
  end

  defp filter_ops([], _key) do
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
end
