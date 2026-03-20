defmodule Microlsm.WalTest do
  use ExUnit.Case, async: false

  alias Microlsm.Wal
  alias Microlsm.Test.Support

  @tag fuzz: true
  test "Wal test" do
    n = 4096

    import StreamData

    x =
      one_of [
        string(:ascii, max_length: 16),
        integer()
      ]

    value =
      one_of [
        constant({}),
        tuple({x})
      ]

    stream = tuple({x, value})

    import StreamData, only: []

    %{data_dir: dir} = Support.setup_datadir()
    filename = Path.join(dir, "wal")
    wal = Wal.open(filename, fn _ -> [] end)

    ops = Enum.take(stream, n)
    Wal.push_batch(wal, ops, n)
    assert ops == Enum.flat_map(Wal.stream(wal), fn {_size, ops} -> ops end)
  end

  test "Key repeats" do
    ops = [x: {1}, x: {}, x: {2}]

    %{data_dir: dir} = Support.setup_datadir()
    filename = Path.join(dir, "wal")
    wal = Wal.open(filename, fn _ -> [] end)

    Wal.push_batch(wal, ops, length(ops))
    assert ops == Enum.flat_map(Wal.stream(wal), fn {_size, ops} -> ops end)
  end
end
