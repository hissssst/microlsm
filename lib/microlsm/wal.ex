defmodule Microlsm.Wal do
  @moduledoc false

  import :erlang, only: [term_to_iovec: 1, binary_to_term: 1, iolist_size: 1]
  import Bitwise, only: [<<<: 2]
  alias Microlsm.SizeException

  # 10MB
  # @batch_read_size 10 * 1024 * 1024
  @batch_read_size 10 * 1024 * 1024

  # Literally infinite size of an operation
  @opsize_size 64

  def push_batch(fd, ops) do
    encoded = for op <- ops, do: encode_op(op)
    :ok = :prim_file.write(fd, encoded)
    :ok = :prim_file.datasync(fd)
  end

  def truncate(fd) do
    {:ok, 0} = :prim_file.position(fd, 0)
    :ok = :prim_file.truncate(fd)
    :ok = :prim_file.sync(fd)
  end

  def stream(fd) do
    Stream.resource(
      fn -> {0, <<>>} end,
      fn {offset, head} ->
        case :prim_file.pread(fd, offset, @batch_read_size) do
          {:ok, tail} ->
            {ops, leftover, offset, length} = decode_ops(head <> tail, [], offset, 0)
            {[{length, ops}], {offset, leftover}}

          :eof when head == <<>> ->
            {:halt, []}
        end
      end,
      fn _ -> [] end
    )
  end

  defp decode_ops(binary, acc, total_size, total_count) do
    case binary do
      <<size::@opsize_size, op::binary-size(size), tail::binary>> ->
        decode_ops(tail, [binary_to_term(op) | acc], total_size + size + 8, total_count + 1)

      binary ->
        {:lists.reverse(acc), binary, total_size + byte_size(binary), total_count}
    end
  end

  defp encode_op(op) do
    encoded = term_to_iovec(op)
    size = iolist_size(encoded)
    SizeException.check!(size, 1 <<< @opsize_size, op)
    [<<size::@opsize_size>> | encoded]
  end
end
