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

  @tag timeout: :infinity
  test "Writes, rewrites and deletes many times", %{name: name, data_dir: data_dir} do
    Microlsm.start_link(
      name: name,
      data_dir: data_dir,
      max_batch_length: 100,
      threshold: 512,
      block_count: 10
    )

    n = 5 * 1024
    shuffled = Enum.shuffle(1..n)

    IO.inspect :timer.tc fn ->
      for i <- shuffled do
        assert :ok = Microlsm.write(name, i, "value_#{i}")
      end

      :ok
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
      assert :error == Microlsm.read(name, i)
    end
  end

  test "Sets and reads in parallel, then recovers", %{name: name, data_dir: data_dir} do
    {:ok, pid} =
      Microlsm.start_link(
        name: name,
        data_dir: data_dir,
        threshold: 1024 * 1024,
        block_count: 1024
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
end
