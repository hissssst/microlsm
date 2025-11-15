defmodule BloomFilterTest do
  use ExUnit.Case, async: true

  alias Microlsm.BloomFilter

  test "Just works" do
    n = 10 * 1024
    bitsize = 10 * n
    k = 6

    filter =
      1..n
      |> Stream.map(fn i -> "key_#{i}" end)
      |> BloomFilter.build(bitsize, k)

    for i <- 1..n do
      assert :maybe == BloomFilter.check(filter, "key_#{i}")
    end

    assert Enum.any?(1..n, fn i ->
             BloomFilter.check(filter, "#{i}_key") == :no
           end)
  end
end
