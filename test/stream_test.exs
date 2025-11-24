defmodule Microlsm.StreamTest do
  use ExUnit.Case, async: true

  test "Just works" do
    assert Enum.to_list(s(0..100)) == Enum.to_list(Microlsm.Stream.merge_streams(s(0..100//2), s(1..100//2)))

    assert Enum.to_list(s(0..100)) == Enum.to_list(Microlsm.Stream.merge_streams(s(1..100), s(0..0)))

    assert Enum.to_list(s(0..100)) == Enum.to_list(Microlsm.Stream.merge_streams(s(0..0), s(0..100)))

    assert Enum.to_list(s(0..100)) == Enum.to_list(Microlsm.Stream.merge_streams(s(0..100), s(0..100)))

    assert [] == Enum.to_list(Microlsm.Stream.merge_streams([], []))

    assert [{0, 0}] == Enum.to_list(Microlsm.Stream.merge_streams([{0, 0}], []))

    assert [{0, 0}] == Enum.to_list(Microlsm.Stream.merge_streams([], [{0, 0}]))

    assert [{0, 1}] == Enum.to_list(Microlsm.Stream.merge_streams([{0, 1}], [{0, 2}]))

    assert Enum.to_list(s(0..10)) == Enum.to_list(Microlsm.Stream.merge_streams(s(0..10), []))

    assert Enum.to_list(s(0..10)) == Enum.to_list(Microlsm.Stream.merge_streams([], s(0..10)))

    assert Enum.to_list(s(0..10)) == Enum.to_list(Microlsm.Stream.merge_streams(s(0..10), s(0..10)))

    assert Enum.to_list(s(0..20)) == Enum.to_list(Microlsm.Stream.merge_streams(s(0..10), s(0..20)))

    assert Enum.to_list(s(0..20)) == Enum.to_list(Microlsm.Stream.merge_streams(s(0..20), s(0..10)))

    assert Enum.to_list(s(0..30)) == Enum.to_list(Microlsm.Stream.merge_streams(Enum.to_list(s(0..20)) ++ [{25, 25}], s(0..30)))

    assert Enum.to_list(s(0..30)) == Enum.to_list(Microlsm.Stream.merge_streams(s(0..30), Enum.to_list(s(0..20)) ++ [{25, 25}]))
  end

  defp s(enumerable) do
    Stream.map(enumerable, fn i -> {i, i} end)
  end
end
