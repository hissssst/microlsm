defmodule MicrolsmTest do
  use ExUnit.Case, async: true

  setup do
    unique = :erlang.unique_integer([:positive])
    name = :"microlsm_#{unique}"
    data_dir = Path.join(System.tmp_dir!(), "microlsm_test_#{unique}")
    File.rm_rf(data_dir)
    File.mkdir(data_dir)

    on_exit(fn -> File.rm_rf(data_dir) end)

    {:ok, name: name, data_dir: data_dir}
  end

  @tag capture_log: true, capture_io: true
  test "Lockfile works", %{name: name, data_dir: data_dir} do
    assert {:ok, pid} = Microlsm.start_link(name: name, data_dir: data_dir)
    refute_receive {:EXIT, _, _}

    Process.flag(:trap_exit, true)
    assert {:error, {%RuntimeError{message: message}, _}} = Microlsm.start_link(name: :other_name, data_dir: data_dir)
    assert message == "Table is in use by #{inspect pid}"

    Process.exit(pid, :kill)
    refute Process.alive?(pid)
    assert_receive {:EXIT, ^pid, :killed}

    assert {:ok, _pid} = Microlsm.start_link(name: name, data_dir: data_dir)
    refute_receive {:EXIT, _, _}
  end

  test "Sets and reads", %{name: name, data_dir: data_dir} do
    Microlsm.start_link(name: name, data_dir: data_dir)
    assert :ok = Microlsm.write(name, "key", "value")
    assert {:ok, "value"} = Microlsm.read(name, "key")
  end

  test "Sets and reads and deletes", %{name: name, data_dir: data_dir} do
    Microlsm.start_link(name: name, data_dir: data_dir)
    assert :ok = Microlsm.write(name, "key", "value")
    assert {:ok, "value"} = Microlsm.read(name, "key")
    assert :ok = Microlsm.delete(name, "key")
    assert :error = Microlsm.read(name, "key")
  end

  test "Recovers on a single key", %{name: name, data_dir: data_dir} do
    {:ok, pid} = Microlsm.start_link(name: name, data_dir: data_dir)
    assert :ok = Microlsm.write(name, "key", "value")
    assert {:ok, "value"} = Microlsm.read(name, "key")

    Process.unlink(pid)
    Process.exit(pid, :kill)
    assert nil == Process.info(pid)

    assert {:ok, pid} = Microlsm.start_link(name: name, data_dir: data_dir)
    assert {:ok, "value"} = Microlsm.read(name, "key")

    assert :ok = Microlsm.delete(name, "key")
    assert :error = Microlsm.read(name, "key")

    Process.unlink(pid)
    Process.exit(pid, :kill)
    assert nil == Process.info(pid)

    assert {:ok, _} = Microlsm.start_link(name: name, data_dir: data_dir)
    assert :error = Microlsm.read(name, "key")
  end

  test "Range reads", %{name: name, data_dir: data_dir} do
    {:ok, _pid} = Microlsm.start_link(name: name, data_dir: data_dir, wal_length_threshold: 9)
    batch = for i <- 1..5, do: {:write, i, i}
    assert :ok = Microlsm.batch(name, batch)

    batch = for i <- 6..10, do: {:write, i, i}
    assert :ok = Microlsm.batch(name, batch)

    batch = for i <- 11..15, do: {:write, i, i}
    assert :ok = Microlsm.batch(name, batch)

    Process.sleep 300

    for i <- 0..20, j <- i..20 do
      microlsm = Enum.to_list(Microlsm.range_read(name, i, j))
      irange = Enum.map(max(i, 1)..min(j, 15)//1, fn i -> {i, i} end)
      assert microlsm == irange
    end
  end

  test "Recovers on a single key rewrite", %{name: name, data_dir: data_dir} do
    {:ok, pid} = Microlsm.start_link(name: name, data_dir: data_dir)
    assert :ok = Microlsm.write(name, "key", "value1")
    assert {:ok, "value1"} = Microlsm.read(name, "key")
    assert :ok = Microlsm.write(name, "key", "value2")
    assert {:ok, "value2"} = Microlsm.read(name, "key")

    Process.unlink(pid)
    Process.exit(pid, :kill)
    assert nil == Process.info(pid)

    assert {:ok, _pid} = Microlsm.start_link(name: name, data_dir: data_dir)
    assert {:ok, "value2"} = Microlsm.read(name, "key")
  end

  @tag timeout: :infinity #, skip: true
  test "Writes, rewrites and deletes many times", %{name: name, data_dir: data_dir} do
    Microlsm.start_link(
      name: name,
      data_dir: data_dir,
      max_batch_length: 100,
      threshold: 4 * 1_024
    )

    times = 1
    n = 10_000
    shuffled = Enum.shuffle(1..n)

    for _ <- 1..times do
      for i <- shuffled do
        assert :ok = Microlsm.write(name, i, "value_#{i}")
      end

      for i <- Enum.shuffle(1..n) do
        assert :error == Microlsm.read(name, -i)
        assert {:ok, "value_#{i}"} == Microlsm.read(name, i)
      end

      for i <- Enum.shuffle(1..n) do
        assert :ok = Microlsm.write(name, i, "other_value_#{i}")
      end

      for i <- Enum.shuffle(1..n) do
        assert {:ok, "other_value_#{i}"} == Microlsm.read(name, i)
      end

      for i <- Enum.shuffle(1..n) do
        assert :ok = Microlsm.delete(name, i)
      end

      for i <- Enum.shuffle(1..n) do
        try do
          Microlsm.read(name, i)
        rescue
          e ->
            IO.inspect i
            reraise e, __STACKTRACE__
        end
      end

      :ok
    end

    Microlsm.Stats.print(name)
  end

  test "Sets and reads in parallel, then recovers", %{name: name, data_dir: data_dir} do
    {:ok, pid} =
      Microlsm.start_link(
        name: name,
        data_dir: data_dir,
        threshold: 1024 * 1024
      )

    n = 10 * 1024

    1..n
    |> Task.async_stream(fn i ->
      assert :ok = Microlsm.write(name, i, "value_#{i}")
    end)
    |> Stream.run()

    1..n
    |> Task.async_stream(fn i ->
      assert :error == Microlsm.read(name, -i)
      assert {:ok, "value_#{i}"} == Microlsm.read(name, i)
    end)
    |> Stream.run()

    Process.unlink(pid)
    Process.exit(pid, :kill)

    assert nil == Process.info(pid)
    Microlsm.start_link(name: name, data_dir: data_dir, threshold: 1024 * 1024, block_count: 1024)

    for i <- Enum.shuffle(1..n) do
      assert :error == Microlsm.read(name, -i)
      assert {:ok, "value_#{i}"} == Microlsm.read(name, i)
    end
  end

  test "all/1", %{name: name, data_dir: data_dir} do
    {:ok, _} =
      Microlsm.start_link(
        name: name,
        data_dir: data_dir,
        threshold: 128
      )

    batch =
      for i <- 1..1024 do
        {:write, i, i}
      end

    Microlsm.batch(name, batch)
    assert Enum.to_list(Microlsm.all(name)) == (for i <- 1..1024, do: {i, i})
  end
end
