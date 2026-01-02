defmodule Microlsm.DescriptorPoolTest do
  use ExUnit.Case, async: true
  alias Microlsm.DescriptorPool

  setup tags do
    count = Map.get(tags, :count, 10)
    pool = DescriptorPool.new()
    state = DescriptorPool.add(pool, __ENV__.file, count)

    {:ok, pool: pool, state: state}
  end

  @tag count: 1
  test "Just works", %{pool: pool, state: state} do
    result =
      DescriptorPool.checkout(pool, state, fn fd ->
        :prim_file.pread(fd, 0, 9)
      end)

    assert result == {:ok, "defmodule"}
  end

  @tag count: 2
  test "Selects next", %{pool: pool, state: state} do
    owner = self()

    spawn(fn ->
      result =
        DescriptorPool.checkout(pool, state, fn _ ->
          send(owner, {:cell, self()})
          receive do :finish -> :finish end
          :some_result
        end)

      send(owner, {:result, result})
    end)

    assert_receive {:cell, cell}

    result =
      DescriptorPool.checkout(pool, state, fn fd ->
        :prim_file.pread(fd, 0, 9)
      end)

    assert result == {:ok, "defmodule"}
    send(cell, :finish)
    assert_receive {:result, :some_result}
  end
end
