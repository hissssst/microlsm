defmodule Microlsm.AfdTest do
  use ExUnit.Case, async: true
  alias Microlsm.Afs

  setup_all do: Microlsm.Fs.init_counters()

  setup do
    i = Integer.to_string :erlang.unique_integer [:positive]
    filename = Path.join [System.tmp_dir!(), "afs_test_#{i}"]
    {:ok, filename: filename}
  end

  test "writes work", %{filename: filename} do
    {:ok, afd} = Afs.open(filename, [:read, :write])
    Afs.pwrite(afd, 0, "hello")
    Afs.close(afd)

    assert "hello" == File.read!(filename)
  end

  test "reads work", %{filename: filename} do
    {:ok, afd} = Afs.open(filename, [:read, :write])
    Afs.pwrite(afd, 0, "hello")
    Afs.close(afd)

    {:ok, afd} = Afs.open(filename, [:read, :write])
    {pread_ref, afd} = Afs.pread(afd, 0, 5)
    assert {{:ok, "hello"}, _} = Afs.receive_pread(afd, pread_ref, 5_000)
  end
end
