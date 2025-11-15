defmodule Microlsm.BloomFilter do
  @moduledoc false

  import Bitwise, only: [|||: 2, <<<: 2, &&&: 2, >>>: 2]

  ## Stream creation

  def new(bitsize, k) do
    bitsize = align_bitsize(bitsize)
    {bitsize, k - 1, 0}
  end

  def add({bitsize, k, mask}, key) do
    mask = add_to_mask(k, mask, key, bitsize)
    {bitsize, k, mask}
  end

  def finalize({bitsize, k, mask}) do
    {bitsize, k, <<mask::little-unsigned-integer-size(bitsize)>>}
  end

  ## Another stream

  def build(enumerable, bitsize, k) when k >= 2 do
    bitsize = align_bitsize(bitsize)

    mask =
      Enum.reduce(enumerable, 0, fn key, mask ->
        add_to_mask(k, mask, key, bitsize)
      end)

    {bitsize, k, <<mask::little-unsigned-integer-size(bitsize)>>}
  end

  ## Checking

  def check(filter, key) do
    {bitsize, k, mask} = filter
    do_check(k - 1, bitsize, mask, key)
  end

  defp do_check(-1, _bitsize, _mask, _key) do
    :maybe
  end

  defp do_check(i, bitsize, mask, key) do
    index = :erlang.phash2({i, key}, bitsize)
    byte_index = div(index, 8)
    bit_in_byte = rem(index, 8)

    byte = :binary.at(mask, byte_index)

    case byte &&& 1 <<< bit_in_byte do
      0 ->
        :no

      _ ->
        do_check(i - 1, bitsize, mask, key)
    end
  end

  ## Helpers

  defp add_to_mask(0, mask, key, bitsize) do
    index = :erlang.phash2({0, key}, bitsize)
    mask ||| 1 <<< index
  end

  defp add_to_mask(k, mask, key, bitsize) do
    index = :erlang.phash2({k, key}, bitsize)
    mask = mask ||| 1 <<< index
    add_to_mask(k - 1, mask, key, bitsize)
  end

  defp align_bitsize(bitsize) do
    align_bytesize(bitsize) <<< 3
  end

  defp align_bytesize(bitsize) do
    bytesize = bitsize >>> 3
    case bitsize &&& 0b111 do
      0 ->
        bytesize

      _ ->
        bytesize + 1
    end
  end
end
